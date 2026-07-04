use anyhow::Result;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, StreamExt};
use twilight_model::gateway::ShardId;

use super::events;

pub async fn connect(
    manager: crate::wasm::loader::PluginManager,
) -> Result<(Shard, tokio::task::JoinHandle<Result<()>>)> {
    let token = std::env::var("DISCORD_TOKEN")?;
    let intents = Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;

    let shard = Shard::new(ShardId::ONE, token, intents);
    let handle = tokio::spawn(async move {
        bot_loop(shard, manager).await
    });

    let placeholder = Shard::new(ShardId::ONE, String::new(), Intents::empty());
    Ok((placeholder, handle))
}

async fn bot_loop(
    mut shard: Shard,
    manager: crate::wasm::loader::PluginManager,
) -> Result<()> {
    tracing::info!("Connecting to Discord gateway...");

    while let Some(item) = shard.next_event(EventTypeFlags::all()).await {
        let event = match item {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(?e, "Gateway receive error");
                continue;
            }
        };

        manager.set_gateway_ping_ms(
            shard
                .latency()
                .average()
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        );

        if let Err(e) = handle_event(event, &manager).await {
            tracing::error!(?e, "Event handler error");
        }
    }

    Ok(())
}

async fn handle_event(
    event: Event,
    manager: &crate::wasm::loader::PluginManager,
) -> Result<()> {
    match event {
        Event::Ready(ready) => {
            tracing::info!(
                user = ?ready.user.name,
                "Bot is ready"
            );

            let payload = serde_json::to_vec(&ready)?;
            manager.dispatch_event("ready", payload, 0, 0).await;
            events::ready::handle(manager).await?;
        }
        Event::MessageCreate(msg) => {
            events::message::handle(&msg, manager).await?;
        }
        Event::InteractionCreate(interaction) => {
            events::interaction::handle(&interaction, manager).await?;
        }
        _ => {}
    }

    Ok(())
}
