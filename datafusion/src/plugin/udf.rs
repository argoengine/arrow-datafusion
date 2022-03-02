use crate::error::{DataFusionError, Result};
use crate::physical_plan::udaf::AggregateUDF;
use crate::physical_plan::udf::ScalarUDF;
use crate::plugin::{Plugin, PluginRegistrar};
use libloading::Library;
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;

/// 定义udf插件，udf的定义方需要实现该trait
pub trait UDFPlugin: Plugin {
    /// get a ScalarUDF by name
    fn get_scalar_udf_by_name(&self, fun_name: &str) -> Result<ScalarUDF>;

    /// return all udf names in the plugin
    fn udf_names(&self) -> Result<Vec<String>>;

    /// get a aggregate udf by name
    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF>;

    /// return all udaf names
    fn udaf_names(&self) -> Result<Vec<String>>;
}

/// UDFPluginManager
#[derive(Default)]
pub struct UDFPluginManager {
    /// scalar udfs
    pub scalar_udfs: HashMap<String, Arc<ScalarUDF>>,

    /// aggregate udfs
    pub aggregate_udfs: HashMap<String, Arc<AggregateUDF>>,

    /// All libraries load from the plugin dir.
    pub libraries: Vec<Arc<Library>>,
}

impl PluginRegistrar for UDFPluginManager {
    fn register_plugin(&mut self, plugin: Box<dyn Plugin>) -> Result<()> {
        if let Some(udf_plugin) = plugin.as_any().downcast_ref::<Box<dyn UDFPlugin>>() {
            udf_plugin
                .udf_names()
                .unwrap()
                .iter()
                .try_for_each(|udf_name| {
                    println!("udf_name:{}", udf_name);
                    if self.scalar_udfs.contains_key(udf_name) {
                        Err(DataFusionError::IoError(io::Error::new(
                            io::ErrorKind::Other,
                            format!("udf name: {} already exists", udf_name),
                        )))
                    } else {
                        let scalar_udf = udf_plugin.get_scalar_udf_by_name(udf_name)?;
                        self.scalar_udfs
                            .insert(udf_name.to_string(), Arc::new(scalar_udf));
                        Ok(())
                    }
                })?;

            udf_plugin
                .udaf_names()
                .unwrap()
                .iter()
                .try_for_each(|udaf_name| {
                    println!("udaf_name:{}", udaf_name);
                    if self.aggregate_udfs.contains_key(udaf_name) {
                        Err(DataFusionError::IoError(io::Error::new(
                            io::ErrorKind::Other,
                            format!("udaf name: {} already exists", udaf_name),
                        )))
                    } else {
                        let aggregate_udf =
                            udf_plugin.get_aggregate_udf_by_name(udaf_name)?;
                        self.aggregate_udfs
                            .insert(udaf_name.to_string(), Arc::new(aggregate_udf));
                        Ok(())
                    }
                })?;
        }
        Err(DataFusionError::IoError(io::Error::new(
            io::ErrorKind::Other,
            format!("expected plugin type is 'dyn UDFPlugin', but it's not"),
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
