wit_bindgen::generate!({
    world: "plugin-world",
    path: "wit",
});

use crate::exports::ynsrvcs::plugins::plugin::Guest;
use crate::ynsrvcs::plugins::host::http_request;

struct PingPlugin;

impl Guest for PingPlugin {
    fn handle_event(
        event_type: String,
        payload: Vec<u8>,
        _guild_id: u64,
        _channel_id: u64,
    ) {
        if event_type != "MESSAGE_CREATE" {
            return;
        }

        let Ok(event) = serde_json::from_slice::<serde_json::Value>(&payload) else {
            return;
        };

        let content = event.get("content").and_then(|v| v.as_str()).unwrap_or("");
        if content.trim() != "!ping" {
            return;
        }

        let channel_id = event
            .get("channel_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if channel_id.is_empty() {
            return;
        }

        let body = serde_json::json!({ "content": "Pong!" }).to_string();
        let _ = http_request(
            "POST",
            &format!("https://discord.com/api/v10/channels/{channel_id}/messages"),
            &body.into_bytes(),
        );
    }
}

export!(PingPlugin);
