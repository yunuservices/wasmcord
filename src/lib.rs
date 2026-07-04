pub mod bot;
pub mod config;
pub mod exception;
pub mod utils;
pub mod wasm;

use anyhow::Result;

pub async fn run() -> Result<()> {
    let _cfg = config::Config::from_env();
    utils::helpers::init_logging()?;

    let engine = wasm::plugin::create_engine()?;
    let plugin_manager = wasm::loader::PluginManager::new(&engine)?;
    plugin_manager.load_all().await?;

    tokio::spawn(wasm::hotreload::watch(plugin_manager.clone()));

    let (_shard, tasks) = bot::handler::connect(plugin_manager).await?;

    tasks.await??;
    Ok(())
}
