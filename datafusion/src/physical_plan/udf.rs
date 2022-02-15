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

//! UDF support

use fmt::{Debug, Formatter};
use std::any::Any;
use std::fmt;

use arrow::datatypes::{DataType, Schema};

use crate::error::Result;
use crate::{logical_plan::Expr, physical_plan::PhysicalExpr};

use super::{
    functions::{ReturnTypeFunction, ScalarFunctionImplementation, Signature},
    type_coercion::coerce,
};
use crate::physical_plan::ColumnarValue;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// 定义udf插件，udf的定义方需要实现该trait
pub trait UDFPlugin: Send + Sync + 'static {
    /// get a ScalarUDF by name
    fn get_scalar_udf_by_name(&self, fun_name: &str) -> Result<ScalarUDF>;

    /// return all udf names in the plugin
    fn udf_names(&self) -> Result<Vec<String>>;
}

/// Every plugin need a UDFPluginDeclaration
#[derive(Copy, Clone)]
pub struct UDFPluginDeclaration {
    /// rustc version of the plugin. The plugin's rustc_version need same as plugin manager.
    pub rustc_version: &'static str,

    /// core version of the plugin. The plugin's core_version need same as plugin manager.
    pub core_version: &'static str,

    /// `register` is a function which impl UDFPluginRegistrar. It will be call when plugin load.
    pub register: unsafe extern "C" fn(&mut dyn UDFPluginRegistrar),
}

/// UDF Plugin Registrar , Define the functions every udf plugin need impl
pub trait UDFPluginRegistrar {
    /// The udf plugin need impl this function
    fn register_udf_plugin(&mut self, plugin_name: &str, function: Box<dyn UDFPlugin>);
}

/// Declare a plugin's name, type and its constructor.
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. And then generating a UDFPluginDeclaration.
/// Therefore you will only be able to declare one plugin per library.
#[macro_export]
macro_rules! declare_udf_plugin {
    ($plugin_name:expr, $plugin_type:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn register_udf_plugin(registrar: &mut dyn UDFPluginRegistrar) {
            // make sure the constructor is the correct type.
            let constructor: fn() -> $plugin_type = $constructor;
            let object = constructor();
            registrar.register_udf_plugin($plugin_name, Box::new(object));
        }

        #[no_mangle]
        pub static udf_plugin_declaration: $crate::UDFPluginDeclaration =
            $crate::UDFPluginDeclaration {
                rustc_version: $crate::RUSTC_VERSION,
                core_version: $crate::CORE_VERSION,
                register: register_udf_plugin,
            };
    };
}

/// Logical representation of a UDF.
#[derive(Clone)]
pub struct ScalarUDF {
    /// name
    pub name: String,
    /// signature
    pub signature: Signature,
    /// Return type
    pub return_type: ReturnTypeFunction,
    /// actual implementation
    ///
    /// The fn param is the wrapped function but be aware that the function will
    /// be passed with the slice / vec of columnar values (either scalar or array)
    /// with the exception of zero param function, where a singular element vec
    /// will be passed. In that case the single element is a null array to indicate
    /// the batch's row count (so that the generative zero-argument function can know
    /// the result array size).
    pub fun: ScalarFunctionImplementation,
}

impl Debug for ScalarUDF {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarUDF")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("fun", &"<FUNC>")
            .finish()
    }
}

impl PartialEq for ScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.signature == other.signature
    }
}

impl PartialOrd for ScalarUDF {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let c = self.name.partial_cmp(&other.name);
        if matches!(c, Some(std::cmp::Ordering::Equal)) {
            self.signature.partial_cmp(&other.signature)
        } else {
            c
        }
    }
}

impl ScalarUDF {
    /// Create a new ScalarUDF
    pub fn new(
        name: &str,
        signature: &Signature,
        return_type: &ReturnTypeFunction,
        fun: &ScalarFunctionImplementation,
    ) -> Self {
        Self {
            name: name.to_owned(),
            signature: signature.clone(),
            return_type: return_type.clone(),
            fun: fun.clone(),
        }
    }

    /// creates a logical expression with a call of the UDF
    /// This utility allows using the UDF without requiring access to the registry.
    pub fn call(&self, args: Vec<Expr>) -> Expr {
        Expr::ScalarUDF {
            fun: Arc::new(self.clone()),
            args,
        }
    }
}

/// Create a physical expression of the UDF.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDF.
pub fn create_physical_expr(
    fun: &ScalarUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // coerce
    let coerced_phy_exprs = coerce(input_phy_exprs, input_schema, &fun.signature)?;

    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ScalarUDFExpr {
        fun: fun.clone(),
        name: fun.name.clone(),
        args: coerced_phy_exprs.clone(),
        return_type: (fun.return_type)(&coerced_exprs_types)?.as_ref().clone(),
    }))
}

/// Physical expression of a UDF.
/// argo engine add
#[derive(Debug)]
pub struct ScalarUDFExpr {
    fun: ScalarUDF,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl ScalarUDFExpr {
    /// create a ScalarUDFExpr
    pub fn new(
        name: &str,
        fun: ScalarUDF,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: &DataType,
    ) -> Self {
        Self {
            fun,
            name: name.to_string(),
            args,
            return_type: return_type.clone(),
        }
    }

    /// return fun
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
    }

    /// return name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// return args
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        &self.return_type
    }
}

impl fmt::Display for ScalarUDFExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.args
                .iter()
                .map(|e| format!("{}", e))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl PhysicalExpr for ScalarUDFExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // evaluate the arguments, if there are no arguments we'll instead pass in a null array
        // indicating the batch size (as a convention)
        // TODO need support zero input arguments
        let inputs = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // evaluate the function
        let fun = self.fun.fun.as_ref();
        (fun)(&inputs)
    }
} // argo engine add end.
