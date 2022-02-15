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

//! This module contains functions and structs supporting user-defined aggregate functions.

use fmt::{Debug, Formatter};
use std::any::Any;
use std::fmt;

use arrow::{
    datatypes::Field,
    datatypes::{DataType, Schema},
};

use crate::physical_plan::PhysicalExpr;
use crate::{error::Result, logical_plan::Expr};

use super::{
    aggregates::AccumulatorFunctionImplementation,
    aggregates::StateTypeFunction,
    expressions::format_state_name,
    functions::{ReturnTypeFunction, Signature},
    type_coercion::coerce,
    Accumulator, AggregateExpr,
};
use std::sync::Arc;

/// 定义udaf插件，udaf的定义方需要实现该trait
pub trait UDAFPlugin: Send + Sync + 'static {
    /// get a aggregate udf by name
    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF>;

    /// return all udaf names
    fn udaf_names(&self) -> Result<Vec<String>>;
}

/// Every plugin need a UDAFPluginDeclaration
#[derive(Copy, Clone)]
pub struct UDAFPluginDeclaration {
    /// rustc version of the plugin. The plugin's rustc_version need same as plugin manager.
    pub rustc_version: &'static str,

    /// core version of the plugin. The plugin's core_version need same as plugin manager.
    pub core_version: &'static str,

    /// `register` is a function which impl UDAFPluginRegistrar. It will be call when plugin load.
    pub register: unsafe extern "C" fn(&mut dyn UDAFPluginRegistrar),
}

/// UDAF Plugin Registrar , Define the functions every udaf plugin need impl
pub trait UDAFPluginRegistrar {
    /// The udaf plugin need impl this function
    fn register_udaf_plugin(&mut self, plugin_name: &str, function: Box<dyn UDAFPlugin>);
}

/// Declare a aggregate udf plugin's name, type and its constructor.
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. And then generating a UDAFPluginDeclaration.
/// Therefore you will only be able to declare one plugin per library.
#[macro_export]
macro_rules! declare_udaf_plugin {
    ($plugin_name:expr, $plugin_type:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn register_plugin(registrar: &mut dyn UDAFPluginRegistrar) {
            // make sure the constructor is the correct type.
            let constructor: fn() -> $plugin_type = $constructor;
            let object = constructor();
            registrar.register_udaf_plugin($plugin_name, Box::new(object));
        }

        #[no_mangle]
        pub static udaf_plugin_declaration: $crate::UDAFPluginDeclaration =
            $crate::UDAFPluginDeclaration {
                rustc_version: $crate::RUSTC_VERSION,
                core_version: $crate::CORE_VERSION,
                register: register_plugin,
            };
    };
}

/// Logical representation of a user-defined aggregate function (UDAF)
/// A UDAF is different from a UDF in that it is stateful across batches.
#[derive(Clone)]
pub struct AggregateUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    pub accumulator: AccumulatorFunctionImplementation,
    /// the accumulator's state's description as a function of the return type
    pub state_type: StateTypeFunction,
}

impl Debug for AggregateUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("AggregateUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for AggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl PartialOrd for AggregateUDF {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let c = self.name.partial_cmp(&other.name);
        if matches!(c, Some(std::cmp::Ordering::Equal)) {
            self.signature.partial_cmp(&other.signature)
        } else {
            c
        }
    }
}

impl AggregateUDF {
    /// Create a new AggregateUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        accumulator: &AccumulatorFunctionImplementation,
        state_type: &StateTypeFunction,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            accumulator: accumulator.clone(),
            state_type: state_type.clone(),
        }
    }

    /// creates a logical expression with a call of the UDAF
    /// This utility allows using the UDAF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::AggregateUDF {
            fun: Arc::new(self.clone()),
            args,
        }
    }
}

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    // coerce
    let coerced_phy_exprs = coerce(input_phy_exprs, input_schema, &fun.signature)?;

    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|arg| arg.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(AggregateFunctionExpr {
        fun: fun.clone(),
        args: coerced_phy_exprs.clone(),
        data_type: (fun.return_type)(&coerced_exprs_types)?.as_ref().clone(),
        name: name.into(),
    }))
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    data_type: DataType,
    name: String,
}

impl AggregateExpr for AggregateFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let fields = (self.fun.state_type)(&self.data_type)?
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(
                    &format_state_name(&self.name, &format!("{}", i)),
                    data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<Field>>();

        Ok(fields)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        (self.fun.accumulator)()
    }

    fn name(&self) -> &str {
        &self.name
    }
}
