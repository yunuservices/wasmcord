use anyhow::Result;

use crate::wasm::loader::PluginManager;

pub async fn handle(_manager: &PluginManager) -> Result<()> {
    Ok(())
}
