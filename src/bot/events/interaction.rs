use anyhow::Result;
use twilight_model::gateway::payload::incoming::InteractionCreate;

use crate::wasm::loader::PluginManager;

pub async fn handle(interaction: &InteractionCreate, manager: &PluginManager) -> Result<()> {
    let payload = serde_json::to_vec(&interaction.0)?;

    let guild_id = interaction
        .0
        .guild_id
        .map(|id| id.get())
        .unwrap_or(0);
    let channel_id = interaction
        .0
        .channel
        .as_ref()
        .map(|c| c.id.get())
        .unwrap_or(0);

    manager
        .dispatch_event("interaction-create", payload, guild_id, channel_id)
        .await;

    Ok(())
}
