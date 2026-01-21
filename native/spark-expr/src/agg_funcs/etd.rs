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

use std::{any::Any, sync::Arc};

use arrow::array::{builder::Int64Builder, cast::AsArray, Array, ArrayRef};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use fusion_sql::udf::{etd::EventTimeDifferential, AccumulatorMode};

/// Bridge to reuse fusion-sql UDAF implementation
#[derive(Debug)]
pub struct EtdBridge {
    inner: EventTimeDifferential,
}

impl EtdBridge {
    pub fn new(mode: AccumulatorMode) -> Self {
        Self {
            inner: EventTimeDifferential::new(mode),
        }
    }
}

impl AggregateUDFImpl for EtdBridge {
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

/// REDUCE_ETD wrapper
#[derive(Debug)]
pub struct ReduceEtd(EtdBridge);

impl ReduceEtd {
    pub fn new(_name: impl Into<String>) -> Self {
        Self(EtdBridge::new(AccumulatorMode::Reduce))
    }
}

impl AggregateUDFImpl for ReduceEtd {
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

/// PARTIAL_ETD wrapper
#[derive(Debug)]
pub struct PartialEtd(EtdBridge);

impl PartialEtd {
    pub fn new(_name: impl Into<String>) -> Self {
        Self(EtdBridge::new(AccumulatorMode::Partial))
    }
}

impl AggregateUDFImpl for PartialEtd {
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

/// FINAL_ETD scalar function reusing fusion-sql logic
#[derive(Debug)]
pub struct FinalEtd {
    signature: Signature,
}

impl Default for FinalEtd {
    fn default() -> Self {
        Self::new()
    }
}

impl FinalEtd {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for FinalEtd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "final_etd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args = &args.args;
        let len = match &args[0] {
            ColumnarValue::Array(a) => a.len(),
            ColumnarValue::Scalar(_) => 1,
        };

        let col = args[0].clone().into_array(len)?;
        let ts = args[1].clone().into_array(len)?;
        let is_recent = args[2].clone().into_array(len)?;

        let ts_arr = ts.as_primitive::<Int64Type>();
        let recent_arr = is_recent.as_boolean();

        let mut builder = Int64Builder::with_capacity(len);

        for i in 0..len {
            if col.is_null(i) || ts_arr.is_null(i) || recent_arr.is_null(i) {
                builder.append_null();
                continue;
            }

            // Robustly extract et_early and et_latest from either ListArray or FixedSizeListArray
            let (et_early, et_latest) = match col.data_type() {
                DataType::List(_) => {
                    let list = col.as_list::<i32>();
                    let pair = list.value(i);
                    let pair_arr = pair.as_primitive::<Int64Type>();
                    if pair_arr.len() < 2 {
                        builder.append_null();
                        continue;
                    }
                    (pair_arr.value(0), pair_arr.value(1))
                }
                DataType::FixedSizeList(_, 2) => {
                    let list = col.as_fixed_size_list();
                    let pair = list.value(i);
                    let pair_arr = pair.as_primitive::<Int64Type>();
                    (pair_arr.value(0), pair_arr.value(1))
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "final_etd expects List or FixedSizeList(2), got {:?}",
                        col.data_type()
                    )));
                }
            };

            let current_ts = ts_arr.value(i);
            let recent = recent_arr.value(i);

            let res = if recent {
                current_ts - et_latest
            } else {
                current_ts - et_early
            };
            builder.append_value(res);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
