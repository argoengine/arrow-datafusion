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

//! DataFusion query execution

use std::fs::DirEntry;
use std::{fs, io};

pub mod context;
pub mod dataframe_impl;
pub(crate) mod disk_manager;
pub(crate) mod memory_manager;
pub mod options;
pub mod runtime_env;
pub mod udaf_plugin_manager;
pub mod udf_plugin_manager;

/// plugin manager trait
pub trait PluginManager {
    /// # Safety
    /// find plugin file from `plugin_path` and load it .
    unsafe fn load(&mut self, plugin_path: String) -> io::Result<()> {
        // find library file from udaf_plugin_path
        let library_files = fs::read_dir(plugin_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        for entry in library_files {
            let entry = entry?;
            let file_type = entry.file_type()?;

            if !file_type.is_file() {
                continue;
            }

            if let Some(path) = entry.path().extension() {
                if let Some(suffix) = path.to_str() {
                    if suffix == "dylib" {
                        self.load_plugin_from_library(&entry)?;
                    }
                }
            }
        }

        Ok(())
    }

    /// # Safety
    /// load plugin from the library `file` . Every different plugins should have different implementations
    unsafe fn load_plugin_from_library(&mut self, file: &DirEntry) -> io::Result<()>;
}
