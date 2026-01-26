// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{
    builder::{Float64Builder, PrimitiveBuilder},
    cast::AsArray,
    types::{Float64Type, Int64Type},
    Array, ArrayRef, ArrowNumericType, Int64Array, PrimitiveArray,
};
use arrow::compute::sum;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{not_impl_err, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    type_coercion::aggregates::avg_return_type, Accumulator, AggregateUDFImpl, ColumnarValue,
    EmitTo, GroupsAccumulator, ReversedUDAF, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::{any::Any, sync::Arc};

use arrow::array::ArrowNativeTypeOp;
use DataType::*;

// Bridge to reuse fusion-sql UDAF implementation for reduce_avg and final_avg
use fusion_sql::udf::{avg::Average, AccumulatorMode};

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct Avg {
    name: String,
    signature: Signature,
    // expr: Arc<dyn PhysicalExpr>,
    input_data_type: DataType,
    result_data_type: DataType,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        let result_data_type = avg_return_type("avg", &data_type).unwrap();

        Self {
            name: name.into(),
            signature: Signature::user_defined(Volatility::Immutable),
            input_data_type: data_type,
            result_data_type,
        }
    }
}

impl AggregateUDFImpl for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::<AvgAccumulator>::default()),
            _ => not_impl_err!(
                "AvgAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "sum"),
                self.input_data_type.clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "count"),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.input_data_type, &self.result_data_type) {
            (Float64, Float64) => Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                &self.input_data_type,
                |sum: f64, count: i64| Ok(sum / count as f64),
            ))),

            _ => not_impl_err!(
                "AvgGroupsAccumulator for ({} --> {})",
                self.input_data_type,
                self.result_data_type
            ),
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(None))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }
}

/// An accumulator to compute the average
#[derive(Debug, Default)]
pub struct AvgAccumulator {
    sum: Option<f64>,
    count: i64,
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.sum),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<Float64Type>();
        self.count += (values.len() - values.null_count()) as i64;
        let v = self.sum.get_or_insert(0.);
        if let Some(x) = sum(values) {
            *v += x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // counts are summed
        self.count += sum(states[1].as_primitive::<Int64Type>()).unwrap_or_default();

        // sums are summed
        if let Some(x) = sum(states[0].as_primitive::<Float64Type>()) {
            let v = self.sum.get_or_insert(0.);
            *v += x;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if self.count == 0 {
            // If all input are nulls, count will be 0 and we will get null after the division.
            // This is consistent with Spark Average implementation.
            Ok(ScalarValue::Float64(None))
        } else {
            Ok(ScalarValue::Float64(
                self.sum.map(|f| f / self.count as f64),
            ))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    /// The type of the returned average
    return_data_type: DataType,

    /// Count per group (use i64 to make Int64Array)
    counts: Vec<i64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Function that computes the final average (value / count)
    avg_fn: F,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    pub fn new(return_data_type: &DataType, avg_fn: F) -> Self {
        Self {
            return_data_type: return_data_type.clone(),
            counts: vec![],
            sums: vec![],
            avg_fn,
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, i64) -> Result<T::Native> + Send,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();
        let data = values.values();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());

        let iter = group_indices.iter().zip(data.iter());
        if values.null_count() == 0 {
            for (&group_index, &value) in iter {
                let sum = &mut self.sums[group_index];
                *sum = (*sum).add_wrapping(value);
                self.counts[group_index] += 1;
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if values.is_null(idx) {
                    continue;
                }
                let sum = &mut self.sums[group_index];
                *sum = (*sum).add_wrapping(value);

                self.counts[group_index] += 1;
            }
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<T>();
        let partial_counts = values[1].as_primitive::<Int64Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        let iter1 = group_indices.iter().zip(partial_counts.values().iter());
        for (&group_index, &partial_count) in iter1 {
            self.counts[group_index] += partial_count;
        }

        // update sums
        self.sums.resize(total_num_groups, T::default_value());
        let iter2 = group_indices.iter().zip(partial_sums.values().iter());
        for (&group_index, &new_value) in iter2 {
            let sum = &mut self.sums[group_index];
            *sum = sum.add_wrapping(new_value);
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let mut builder = PrimitiveBuilder::<T>::with_capacity(sums.len());
        let iter = sums.into_iter().zip(counts);

        for (sum, count) in iter {
            if count != 0 {
                builder.append_value((self.avg_fn)(sum, count)?)
            } else {
                builder.append_null();
            }
        }
        let array: PrimitiveArray<T> = builder.finish();

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts = Int64Array::new(counts.into(), None);

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), None)
            .with_data_type(self.return_data_type.clone());

        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<i64>()
            + self.sums.capacity() * std::mem::size_of::<T>()
    }
}

/// Bridge to reuse fusion-sql UDAF implementation
#[derive(Debug)]
pub struct AvgBridge {
    inner: Average,
}

impl AvgBridge {
    pub fn new(mode: AccumulatorMode) -> Self {
        Self {
            inner: Average::new(mode),
        }
    }
}

impl AggregateUDFImpl for AvgBridge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types).map_err(to_df_err)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        self.inner.state_fields(args).map_err(to_df_err)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args).map_err(to_df_err)
    }
}

fn to_df_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::External(format!("{}", e).into())
}

/// REDUCE_AVG wrapper
#[derive(Debug)]
pub struct ReduceAvg(AvgBridge);

impl ReduceAvg {
    pub fn new(_name: impl Into<String>) -> Self {
        Self(AvgBridge::new(AccumulatorMode::Reduce))
    }
}

impl AggregateUDFImpl for ReduceAvg {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn signature(&self) -> &Signature {
        self.0.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.return_type(arg_types)
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        self.0.state_fields(args)
    }
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.0.accumulator(acc_args)
    }
}

/// FINAL_AVG wrapper that reuses fusion-sql implementation
/// This wrapper handles the conversion from List to FixedSizeList
#[derive(Debug)]
pub struct FinalAvg(AvgBridge);

impl FinalAvg {
    pub fn new() -> Self {
        Self(AvgBridge::new(AccumulatorMode::Final))
    }
}

impl Default for FinalAvg {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for FinalAvg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        self.0.state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Get the inner accumulator from fusion_sql
        let inner = self.0.accumulator(acc_args)?;
        // Wrap it to handle List -> FixedSizeList conversion
        Ok(Box::new(FinalAvgAccumulatorWrapper { inner }))
    }
}

/// Wrapper accumulator that converts List to FixedSizeList before calling fusion_sql
#[derive(Debug)]
struct FinalAvgAccumulatorWrapper {
    inner: Box<dyn Accumulator>,
}

impl Accumulator for FinalAvgAccumulatorWrapper {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Convert List to FixedSizeList if needed
        let converted_values: Vec<ArrayRef> = values
            .iter()
            .map(|arr| {
                match arr.data_type() {
                    DataType::List(_) => {
                        // Convert List to FixedSizeList
                        let list = arr.as_list::<i32>();
                        let mut builder = arrow::array::FixedSizeListBuilder::new(
                            arrow::array::Float64Builder::new(),
                            2,
                        );
                        
                        for i in 0..list.len() {
                            if list.is_null(i) {
                                // For null elements, append 2 null values
                                builder.values().append_null();
                                builder.values().append_null();
                                builder.append(false);
                            } else {
                                let value = list.value(i);
                                let value_arr = value.as_primitive::<Float64Type>();
                                if value_arr.len() >= 2 {
                                    builder.values().append_value(value_arr.value(0));
                                    builder.values().append_value(value_arr.value(1));
                                    builder.append(true);
                                } else {
                                    // For elements with insufficient length, append nulls
                                    builder.values().append_null();
                                    builder.values().append_null();
                                    builder.append(false);
                                }
                            }
                        }
                        Arc::new(builder.finish()) as ArrayRef
                    }
                    _ => Arc::clone(arr),
                }
            })
            .collect();
        
        self.inner.update_batch(&converted_values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }
}
