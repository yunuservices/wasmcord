use anyhow::Result;
use twilight_model::gateway::payload::incoming::MessageCreate;

use crate::wasm::loader::PluginManager;

pub async fn handle(msg: &MessageCreate, manager: &PluginManager) -> Result<()> {
    let message = &msg.0;

    if message.author.bot {
        return Ok(());
    }

    tracing::debug!(
        content = %message.content,
        channel = %message.channel_id,
        "Message received"
    );

    manager
        .dispatch_event(
            "message-create",
            message.content.clone().into_bytes(),
            message.guild_id.map(|id| id.get()).unwrap_or(0),
            message.channel_id.get(),
        )
        .await;

    Ok(())
}
