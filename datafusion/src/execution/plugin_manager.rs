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

//! plugin manager

use log::info;
use std::fs::DirEntry;
use std::{env, fs, io};

/// plugin manager trait
pub trait PluginManager {
    /// # Safety
    /// find plugin file from `plugin_path` and load it .
    unsafe fn load(&mut self, plugin_path: String) -> io::Result<()> {
        // find library file from udaf_plugin_path
        info!("load plugin from dir:{}", plugin_path);
        println!("load plugin from dir:{}", plugin_path);
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
                        info!("load plugin from library file:{}", path.to_str().unwrap());
                        println!(
                            "load plugin from library file:{}",
                            path.to_str().unwrap()
                        );
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

/// get the plugin dir
pub fn plugin_dir() -> String {
    let current_exe_dir = match env::current_exe() {
        Ok(exe_path) => exe_path.display().to_string(),
        Err(_e) => "".to_string(),
    };

    // If current_exe_dir contain `deps` the root dir is the parent dir
    // eg: /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/deps/plugins_app-067452b3ff2af70e
    // the plugin dir is /Users/xxx/workspace/rust/rust_plugin_sty/target/debug
    // else eg: /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/plugins_app
    // the plugin dir is /Users/xxx/workspace/rust/rust_plugin_sty/target/debug/
    if current_exe_dir.contains("/deps/") {
        let i = current_exe_dir.find("/deps/").unwrap();
        String::from(&current_exe_dir.as_str()[..i])
    } else {
        let i = current_exe_dir.rfind('/').unwrap();
        String::from(&current_exe_dir.as_str()[..i])
    }
}
