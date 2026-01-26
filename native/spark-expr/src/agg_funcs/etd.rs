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

use arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDFImpl, Signature, Volatility,
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

/// FINAL_ETD wrapper that reuses fusion-sql implementation
#[derive(Debug)]
pub struct FinalEtd(EtdBridge);

impl FinalEtd {
    pub fn new() -> Self {
        Self(EtdBridge::new(AccumulatorMode::Final))
    }
}

impl Default for FinalEtd {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for FinalEtd {
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
