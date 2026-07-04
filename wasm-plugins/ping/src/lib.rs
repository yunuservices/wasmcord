wit_bindgen::generate!({
    world: "plugin-world",
    path: "wit",
});

use crate::exports::ynsrvcs::plugins::plugin::{Command, EventType, Guest};
use crate::ynsrvcs::plugins::host::send_message;

struct PingPlugin;

impl Guest for PingPlugin {
    fn init() -> Vec<Command> {
        vec![Command {
            name: "ping".into(),
            description: "Ping the bot".into(),
        }]
    }

    fn handle_event(
        _event_type: EventType,
        payload: Vec<u8>,
        _guild_id: u64,
        channel_id: u64,
    ) {
        let msg = String::from_utf8_lossy(&payload);
        if msg.trim() == "!ping" {
            let _ = send_message(channel_id, "Pong!");
        }
    }
}

export!(PingPlugin);
