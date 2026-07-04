wit_bindgen::generate!({
    world: "plugin-world",
    path: "wit",
});

use crate::exports::ynsrvcs::plugins::plugin::{EventType, Guest};
use crate::ynsrvcs::plugins::host::http_request;

struct PingPlugin;

impl Guest for PingPlugin {
    fn handle_event(
        event_type: EventType,
        payload: Vec<u8>,
        _guild_id: u64,
        channel_id: u64,
    ) {
        if event_type != EventType::MessageCreate {
            return;
        }

        let msg = String::from_utf8_lossy(&payload);
        if msg.trim() != "!ping" {
            return;
        }

        let body = serde_json::json!({ "content": "Pong!" }).to_string();
        let path = format!("/channels/{channel_id}/messages");
        let _ = http_request("POST", &format!("https://discord.com/api/v10{path}"), &body.into_bytes());
    }
}

export!(PingPlugin);
