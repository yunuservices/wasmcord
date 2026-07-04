use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuildConfig {
    pub guild_id: i64,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginData {
    pub guild_id: i64,
    pub plugin_name: String,
    pub key: String,
    pub value: Vec<u8>,
}
