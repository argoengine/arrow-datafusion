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

//! udaf plugin manager
//!
use crate::physical_plan::udaf::UDAFPluginRegistrar as UDAFPluginRegistrarTrait;
use crate::physical_plan::udaf::{AggregateUDF, UDAFPlugin, UDAFPluginDeclaration};
use lazy_static::lazy_static;
use libloading::Library;
use std::collections::HashMap;
use std::fs::DirEntry;
use std::io;
use std::sync::Arc;

use crate::error::Result;
use crate::execution::PluginManager;
use crate::physical_plan::{CORE_VERSION, RUSTC_VERSION};
lazy_static! {
    /// load all udaf plugin
    pub static ref UDAF_PLUGIN_MANAGER: UDAFPluginManager = unsafe {
        let mut plugin = UDAFPluginManager::default();
        plugin.load("plugin/udaf".to_string()).unwrap();
        plugin
    };
}

/// UDAFPluginManager
#[derive(Default)]
pub struct UDAFPluginManager {
    /// aggregate udf plugins save as udaf_name:UDAFPluginProxy
    pub aggregate_udf_plugins: HashMap<String, Arc<UDAFPluginProxy>>,

    /// Every Library need a plugin_name .
    pub plugin_names: Vec<String>,

    /// All libraries load from the plugin dir.
    pub libraries: Vec<Arc<Library>>,
}

impl PluginManager for UDAFPluginManager {
    unsafe fn load_plugin_from_library(&mut self, file: &DirEntry) -> io::Result<()> {
        // load the library into memory
        let library = Library::new(file.path())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let library = Arc::new(library);

        // get a pointer to the plugin_declaration symbol.
        let dec = library
            .get::<*mut UDAFPluginDeclaration>(b"udaf_plugin_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if dec.rustc_version != RUSTC_VERSION || dec.core_version != CORE_VERSION {
            return Err(io::Error::new(io::ErrorKind::Other, "Version mismatch"));
        }

        let mut registrar = UDAFPluginRegistrar::new(library.clone());
        (dec.register)(&mut registrar);

        // Check for duplicate plugin_name and UDF names
        if let Some(udaf_plugin_proxy) = registrar.udaf_plugin_proxy {
            if self.plugin_names.contains(&udaf_plugin_proxy.plugin_name) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "plugin name: {} already exists",
                        udaf_plugin_proxy.plugin_name
                    ),
                ));
            }

            udaf_plugin_proxy
                .aggregate_udf_plugin
                .udaf_names()
                .unwrap()
                .iter()
                .try_for_each(|udaf_name| {
                    if self.aggregate_udf_plugins.contains_key(udaf_name) {
                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "udaf name: {} already exists in plugin: {}",
                                udaf_name, udaf_plugin_proxy.plugin_name
                            ),
                        ))
                    } else {
                        self.aggregate_udf_plugins.insert(
                            udaf_name.to_string(),
                            Arc::new(udaf_plugin_proxy.clone()),
                        );
                        Ok(())
                    }
                })?;

            self.plugin_names.push(udaf_plugin_proxy.plugin_name);
        }

        self.libraries.push(library);
        Ok(())
    }
}

/// A proxy object which wraps a [`UDAFPlugin`] and makes sure it can't outlive
/// the library it came from.
#[derive(Clone)]
pub struct UDAFPluginProxy {
    /// One UDAFPluginProxy only have one UDAFPlugin
    aggregate_udf_plugin: Arc<Box<dyn UDAFPlugin>>,

    /// Library
    _lib: Arc<Library>,

    /// One Library can only have one plugin
    plugin_name: String,
}

impl UDAFPlugin for UDAFPluginProxy {
    fn get_aggregate_udf_by_name(&self, fun_name: &str) -> Result<AggregateUDF> {
        self.aggregate_udf_plugin
            .get_aggregate_udf_by_name(fun_name)
    }

    fn udaf_names(&self) -> Result<Vec<String>> {
        self.aggregate_udf_plugin.udaf_names()
    }
}

/// impl UDAFPluginRegistrarTrait
struct UDAFPluginRegistrar {
    udaf_plugin_proxy: Option<UDAFPluginProxy>,
    lib: Arc<Library>,
}

impl UDAFPluginRegistrar {
    pub fn new(lib: Arc<Library>) -> Self {
        Self {
            udaf_plugin_proxy: None,
            lib,
        }
    }
}

impl UDAFPluginRegistrarTrait for UDAFPluginRegistrar {
    fn register_udaf_plugin(&mut self, plugin_name: &str, plugin: Box<dyn UDAFPlugin>) {
        let proxy = UDAFPluginProxy {
            aggregate_udf_plugin: Arc::new(plugin),
            _lib: self.lib.clone(),
            plugin_name: plugin_name.to_string(),
        };

        self.udaf_plugin_proxy = Some(proxy);
    }
}
