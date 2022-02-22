use crate::error::Result;
use crate::plugin::udf::UDFPluginManager;
use std::any::Any;
use std::env;

/// plugin manager
pub mod plugin_manager;
/// udf plugin
pub mod udf;

/// CARGO_PKG_VERSION
pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
/// RUSTC_VERSION
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

/// Top plugin trait
pub trait Plugin {
    /// Returns the plugin as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// The enum of Plugin
#[derive(PartialEq, std::cmp::Eq, std::hash::Hash, Copy, Clone)]
pub enum PluginEnum {
    /// UDF/UDAF plugin
    UDF,
}

impl PluginEnum {
    /// new a struct which impl the PluginRegistrar trait
    pub fn init_plugin_manager(&self) -> Box<dyn PluginRegistrar> {
        match self {
            PluginEnum::UDF => Box::new(UDFPluginManager::default()),
        }
    }
}

/// Every plugin need a PluginDeclaration
#[derive(Copy, Clone)]
pub struct PluginDeclaration {
    /// rustc version of the plugin. The plugin's rustc_version need same as plugin manager.
    pub rustc_version: &'static str,

    /// core version of the plugin. The plugin's core_version need same as plugin manager.
    pub core_version: &'static str,

    /// One of PluginEnum
    pub plugin_type: unsafe extern "C" fn() -> PluginEnum,

    /// `register` is a function which impl PluginRegistrar. It will be call when plugin load.
    pub register: unsafe extern "C" fn(&mut Box<dyn PluginRegistrar>),
}

/// Plugin Registrar , Every plugin need implement this trait
pub trait PluginRegistrar: Send + Sync + 'static {
    /// The implementer of the plug-in needs to call this interface to report his own information to the plug-in manager
    fn register_plugin(&mut self, plugin: Box<dyn Plugin>) -> Result<()>;

    /// Returns the plugin registrar as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// Declare a plugin's PluginDeclaration.
///
/// # Notes
///
/// This works by automatically generating an `extern "C"` function with a
/// pre-defined signature and symbol name. And then generating a PluginDeclaration.
/// Therefore you will only be able to declare one plugin per library.
#[macro_export]
macro_rules! declare_plugin {
    ($plugin_type:expr, $curr_plugin_type:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn register_plugin(
            registrar: &mut Box<dyn $crate::plugin::PluginRegistrar>,
        ) {
            // make sure the constructor is the correct type.
            let constructor: fn() -> $curr_plugin_type = $constructor;
            let object = constructor();
            registrar.register_plugin(Box::new(object));
        }

        #[no_mangle]
        pub extern "C" fn get_plugin_type() -> $crate::plugin::PluginEnum {
            $plugin_type
        }

        #[no_mangle]
        pub static plugin_declaration: $crate::plugin::PluginDeclaration =
            $crate::plugin::PluginDeclaration {
                rustc_version: $crate::plugin::RUSTC_VERSION,
                core_version: $crate::plugin::CORE_VERSION,
                plugin_type: get_plugin_type,
                register: register_plugin,
            };
    };
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
