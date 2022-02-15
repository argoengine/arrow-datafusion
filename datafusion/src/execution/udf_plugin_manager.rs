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

//! udf plugin manager
//!
use crate::physical_plan::udf::UDFPluginRegistrar as UDFPluginRegistrarTrait;
use crate::physical_plan::udf::{ScalarUDF, UDFPlugin, UDFPluginDeclaration};
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
    /// load all udf plugin
    pub static ref UDF_PLUGIN_MANAGER: UDFPluginManager = unsafe {
        let mut plugin = UDFPluginManager::default();
        plugin.load("plugin/udf".to_string()).unwrap();
        plugin
    };
}

/// UDFPluginManager
#[derive(Default)]
pub struct UDFPluginManager {
    /// scalar udf plugins save as udaf_name:UDFPluginProxy
    pub scalar_udfs: HashMap<String, Arc<UDFPluginProxy>>,

    /// Every Library need a plugin_name .
    pub plugin_names: Vec<String>,

    /// All libraries load from the plugin dir.
    pub libraries: Vec<Arc<Library>>,
}

impl PluginManager for UDFPluginManager {
    unsafe fn load_plugin_from_library(&mut self, file: &DirEntry) -> io::Result<()> {
        // load the library into memory
        let library = Library::new(file.path())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let library = Arc::new(library);

        // get a pointer to the plugin_declaration symbol.
        let dec = library
            .get::<*mut UDFPluginDeclaration>(b"udf_plugin_declaration\0")
            .unwrap()
            .read();

        // version checks to prevent accidental ABI incompatibilities
        if dec.rustc_version != RUSTC_VERSION.as_str() || dec.core_version != CORE_VERSION
        {
            return Err(io::Error::new(io::ErrorKind::Other, "Version mismatch"));
        }

        let mut registrar = UDFPluginRegistrar::new(library.clone());
        (dec.register)(&mut registrar);

        // Check for duplicate plugin_name and UDF names
        if let Some(udf_plugin_proxy) = registrar.udf_plugin_proxy {
            if self.plugin_names.contains(&udf_plugin_proxy.plugin_name) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "plugin name: {} already exists",
                        udf_plugin_proxy.plugin_name
                    ),
                ));
            }

            udf_plugin_proxy
                .scalar_udf_plugin
                .udf_names()
                .unwrap()
                .iter()
                .try_for_each(|udf_name| {
                    if self.scalar_udfs.contains_key(udf_name) {
                        Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "udf name: {} already exists in plugin: {}",
                                udf_name, udf_plugin_proxy.plugin_name
                            ),
                        ))
                    } else {
                        self.scalar_udfs.insert(
                            udf_name.to_string(),
                            Arc::new(udf_plugin_proxy.clone()),
                        );
                        Ok(())
                    }
                })?;

            self.plugin_names.push(udf_plugin_proxy.plugin_name);
        }

        self.libraries.push(library);
        Ok(())
    }
}

/// A proxy object which wraps a [`UDFPlugin`] and makes sure it can't outlive
/// the library it came from.

#[derive(Clone)]
pub struct UDFPluginProxy {
    scalar_udf_plugin: Arc<Box<dyn UDFPlugin>>,
    _lib: Arc<Library>,
    plugin_name: String,
}

impl UDFPlugin for UDFPluginProxy {
    fn get_scalar_udf_by_name(&self, fun_name: &str) -> Result<ScalarUDF> {
        self.scalar_udf_plugin.get_scalar_udf_by_name(fun_name)
    }

    fn udf_names(&self) -> Result<Vec<String>> {
        self.scalar_udf_plugin.udf_names()
    }
}

struct UDFPluginRegistrar {
    udf_plugin_proxy: Option<UDFPluginProxy>,
    lib: Arc<Library>,
}

impl UDFPluginRegistrar {
    pub fn new(lib: Arc<Library>) -> Self {
        Self {
            udf_plugin_proxy: None,
            lib,
        }
    }
}

impl UDFPluginRegistrarTrait for UDFPluginRegistrar {
    fn register_udf_plugin(&mut self, plugin_name: &str, plugin: Box<dyn UDFPlugin>) {
        let proxy = UDFPluginProxy {
            scalar_udf_plugin: Arc::new(plugin),
            _lib: self.lib.clone(),
            plugin_name: plugin_name.to_string(),
        };

        self.udf_plugin_proxy = Some(proxy);
    }
}
