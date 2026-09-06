use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use songbird::Songbird;
use tokio::sync::Mutex as AsyncMutex;
use twilight_gateway::MessageSender;
use twilight_model::gateway::payload::outgoing::{
    RequestGuildMembers, UpdatePresence, UpdateVoiceState,
};
use twilight_model::gateway::presence::{Activity, ActivityType, Status};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

use super::config::{PluginConfig, PluginResourceLimiter};
use super::kv::KvStore;
use super::loader::{
    DISCORD_RATE_LIMIT_MAX_CONCURRENT, DISCORD_REQUEST_MAX_RETRIES, HTTP_TIMEOUT, ScheduleCmd,
};
use super::net::{check_outbound_url, is_discord_api_url};
use super::plugin;
use super::workspace::{workspace_read, workspace_write};

#[derive(Clone)]
pub(crate) struct BusMessage {
    pub(crate) topic: String,
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct DiscordRateLimiter {
    global_reset: Arc<AtomicU64>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl DiscordRateLimiter {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            global_reset: Arc::new(AtomicU64::new(0)),
            semaphore: Arc::new(tokio::sync::Semaphore::new(max_concurrent)),
        }
    }

    pub async fn acquire(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.semaphore
            .acquire()
            .await
            .expect("semaphore should not be closed")
    }

    pub async fn wait_for_reset(&self) {
        let reset_ms = self.global_reset.load(Ordering::Relaxed);
        if reset_ms == 0 {
            return;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        if reset_ms > now {
            tokio::time::sleep(Duration::from_millis(reset_ms - now)).await;
        }
    }

    pub fn mark_rate_limited(&self, retry_after_seconds: u64) {
        let reset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            + retry_after_seconds.saturating_mul(1000)
            + 100; // small buffer
        self.global_reset.store(reset, Ordering::Relaxed);
    }
}

static GLOBAL_DISCORD_RATE_LIMITER: OnceLock<DiscordRateLimiter> = OnceLock::new();

pub struct HostContext {
    wasi: WasiCtx,
    table: wasmtime::component::ResourceTable,
    client: reqwest::Client,
    rate_limiter: &'static DiscordRateLimiter,
    gateway_ping_ms: Arc<AtomicU64>,
    application_id: Arc<AtomicU64>,
    shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
    shard_count: Arc<AtomicU64>,
    songbird: Arc<AsyncMutex<Option<Songbird>>>,
    bus_subscriptions: Arc<AsyncMutex<HashMap<String, HashSet<String>>>>,
    bus_queue: Arc<AsyncMutex<HashMap<String, VecDeque<BusMessage>>>>,
    schedule_tx: tokio::sync::mpsc::UnboundedSender<ScheduleCmd>,
    plugin_name: String,
    kv: KvStore,
    workspace: PathBuf,
    config: PluginConfig,
    pub(crate) limiter: PluginResourceLimiter,
}

impl HostContext {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        gateway_ping_ms: Arc<AtomicU64>,
        application_id: Arc<AtomicU64>,
        shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
        shard_count: Arc<AtomicU64>,
        songbird: Arc<AsyncMutex<Option<Songbird>>>,
        bus_subscriptions: Arc<AsyncMutex<HashMap<String, HashSet<String>>>>,
        bus_queue: Arc<AsyncMutex<HashMap<String, VecDeque<BusMessage>>>>,
        schedule_tx: tokio::sync::mpsc::UnboundedSender<ScheduleCmd>,
        plugin_name: String,
        kv: KvStore,
        workspace: PathBuf,
        config: PluginConfig,
    ) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().build(),
            table: wasmtime::component::ResourceTable::default(),
            client: reqwest::Client::new(),
            rate_limiter: GLOBAL_DISCORD_RATE_LIMITER
                .get_or_init(|| DiscordRateLimiter::new(DISCORD_RATE_LIMIT_MAX_CONCURRENT)),
            gateway_ping_ms,
            application_id,
            shard_senders,
            shard_count,
            songbird,
            bus_subscriptions,
            bus_queue,
            schedule_tx,
            plugin_name,
            kv,
            workspace,
            limiter: PluginResourceLimiter::new(config.limits),
            config,
        }
    }

    async fn send_discord_request(
        &mut self,
        request: reqwest::Request,
    ) -> Result<reqwest::Response, String> {
        let _permit = self.rate_limiter.acquire().await;
        let mut last_error: Option<String> = None;

        for attempt in 0..=DISCORD_REQUEST_MAX_RETRIES {
            self.rate_limiter.wait_for_reset().await;
            let request = request
                .try_clone()
                .ok_or_else(|| "request body is not retryable".to_string())?;

            match self.client.execute(request).await {
                Ok(response) => {
                    if response.status() == 429 {
                        let retry_after = response
                            .headers()
                            .get("retry-after")
                            .and_then(|v| v.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(1);
                        self.rate_limiter.mark_rate_limited(retry_after);
                        if attempt == DISCORD_REQUEST_MAX_RETRIES {
                            return Ok(response);
                        }
                        last_error = Some(format!("rate limited (retry after {retry_after}s)"));
                        continue;
                    }
                    return Ok(response);
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    if attempt < DISCORD_REQUEST_MAX_RETRIES {
                        let delay = Duration::from_millis(200 * 2_u64.pow(attempt));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "discord request failed".to_string()))
    }
}

impl wasmtime::component::HasData for HostContext {
    type Data<'a> = &'a mut Self;
}

impl WasiView for HostContext {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl plugin::ynsrvcs::plugins::host::Host for HostContext {
    async fn http_request(
        &mut self,
        method: String,
        url: String,
        body: Vec<u8>,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;

        let is_discord = is_discord_api_url(&url);
        if !is_discord {
            check_outbound_url(&url, &self.config.permissions.http_allowed_hosts).await?;
        }
        let mut req_builder = self.client.request(method, &url);
        if is_discord {
            if let Ok(token) = std::env::var("DISCORD_TOKEN") {
                req_builder = req_builder.header("Authorization", format!("Bot {token}"));
            }
            if !body.is_empty() {
                req_builder = req_builder.header("Content-Type", "application/json");
            }
        }

        let req = req_builder.body(body).build().map_err(|e| e.to_string())?;

        let resp = if is_discord {
            self.send_discord_request(req).await?
        } else {
            tokio::time::timeout(HTTP_TIMEOUT, self.client.execute(req))
                .await
                .map_err(|_| "http request timed out".to_string())?
                .map_err(|e| e.to_string())?
        };

        let status = resp.status().as_u16();
        let body = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            let text = String::from_utf8_lossy(&body);
            tracing::warn!("http_request returned {status} for {url}: {text}");
        }

        Ok(plugin::ynsrvcs::plugins::host::Response { status, body })
    }

    async fn send_channel_message_with_attachments(
        &mut self,
        channel_id: u64,
        content: String,
        attachments: Vec<plugin::ynsrvcs::plugins::host::Attachment>,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");

        let attachment_meta: Vec<serde_json::Value> = attachments
            .iter()
            .enumerate()
            .map(|(idx, a)| {
                serde_json::json!({
                    "id": idx.to_string(),
                    "filename": a.filename,
                    "description": "",
                })
            })
            .collect();
        let payload = serde_json::json!({
            "content": content,
            "attachments": attachment_meta,
        })
        .to_string();

        let mut form = reqwest::multipart::Form::new().text("payload_json", payload);
        for (idx, attachment) in attachments.into_iter().enumerate() {
            let part = reqwest::multipart::Part::bytes(attachment.data)
                .file_name(attachment.filename)
                .mime_str(&attachment.content_type)
                .map_err(|e| e.to_string())?;
            form = form.part(format!("files[{idx}]"), part);
        }

        let req = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .multipart(form)
            .build()
            .map_err(|e| e.to_string())?;

        let _permit = self.rate_limiter.acquire().await;
        self.rate_limiter.wait_for_reset().await;
        let resp = tokio::time::timeout(HTTP_TIMEOUT, self.client.execute(req))
            .await
            .map_err(|_| "http request timed out".to_string())?
            .map_err(|e| e.to_string())?;
        if resp.status() == 429 {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(1);
            self.rate_limiter.mark_rate_limited(retry_after);
        }

        let status = resp.status().as_u16();
        let body = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            let text = String::from_utf8_lossy(&body);
            tracing::warn!(
                "send_channel_message_with_attachments returned {status} for {url}: {text}"
            );
        }

        Ok(plugin::ynsrvcs::plugins::host::Response { status, body })
    }

    async fn send_channel_message_with_components(
        &mut self,
        channel_id: u64,
        content: String,
        components: String,
    ) -> Result<plugin::ynsrvcs::plugins::host::Response, String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = if components.trim().is_empty() {
            serde_json::Value::Array(Vec::new())
        } else {
            serde_json::from_str(&components)
                .map_err(|e| format!("invalid components json: {e}"))?
        };

        let body = serde_json::json!({
            "content": content,
            "components": components_json,
        });

        let url = format!("https://discord.com/api/v10/channels/{channel_id}/messages");
        let req = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| e.to_string())?;
        let resp = self.send_discord_request(req).await?;

        let status = resp.status().as_u16();
        let body_bytes = resp.bytes().await.map_err(|e| e.to_string())?.to_vec();
        if status >= 400 {
            tracing::warn!(
                status,
                url,
                text = %String::from_utf8_lossy(&body_bytes),
                "send_channel_message_with_components failed"
            );
        }

        Ok(plugin::ynsrvcs::plugins::host::Response {
            status,
            body: body_bytes,
        })
    }

    async fn reply_to_interaction(
        &mut self,
        interaction_id: u64,
        interaction_token: String,
        content: String,
        ephemeral: bool,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let mut data = serde_json::json!({
            "content": content,
        });
        if ephemeral {
            data["flags"] = 64.into();
        }

        let body = serde_json::json!({
            "type": 4,
            "data": data,
        });

        let url = format!(
            "https://discord.com/api/v10/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let req = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| e.to_string())?;
        let resp = self.send_discord_request(req).await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!("interaction reply failed ({status}): {text}"))
        }
    }

    async fn edit_interaction_message(
        &mut self,
        interaction_token: String,
        content: String,
        components: String,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let app_id = self.application_id.load(Ordering::Relaxed).to_string();
        if app_id == "0" {
            return Err("application id not available".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = if components.trim().is_empty() {
            serde_json::Value::Array(Vec::new())
        } else {
            serde_json::from_str(&components)
                .map_err(|e| format!("invalid components json: {e}"))?
        };

        let body = serde_json::json!({
            "content": content,
            "components": components_json,
        });

        let url = format!(
            "https://discord.com/api/v10/webhooks/{app_id}/{interaction_token}/messages/@original"
        );
        let req = self
            .client
            .patch(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| e.to_string())?;
        let resp = self.send_discord_request(req).await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!(
                "edit interaction message failed ({status}): {text}"
            ))
        }
    }

    async fn show_modal(
        &mut self,
        interaction_id: u64,
        interaction_token: String,
        title: String,
        custom_id: String,
        components: String,
    ) -> Result<(), String> {
        if !self.config.permissions.http {
            return Err("http requests are not permitted".to_string());
        }

        let token =
            std::env::var("DISCORD_TOKEN").map_err(|_| "DISCORD_TOKEN not set".to_string())?;
        let components_json: serde_json::Value = serde_json::from_str(&components)
            .map_err(|e| format!("invalid components json: {e}"))?;

        let body = serde_json::json!({
            "type": 9,
            "data": {
                "custom_id": custom_id,
                "title": title,
                "components": components_json,
            }
        });

        let url = format!(
            "https://discord.com/api/v10/interactions/{interaction_id}/{interaction_token}/callback"
        );
        let req = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .build()
            .map_err(|e| e.to_string())?;
        let resp = self.send_discord_request(req).await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            Err(format!("show modal failed ({status}): {text}"))
        }
    }

    async fn get_env(&mut self, name: String) -> Option<String> {
        if !self.config.permissions.env.iter().any(|k| k == &name) {
            return None;
        }

        std::env::var(&name).ok().filter(|v| !v.is_empty())
    }

    async fn gateway_ping(&mut self) -> u64 {
        self.gateway_ping_ms.load(Ordering::Relaxed)
    }

    async fn schedule_task(&mut self, name: String, interval_ms: u64) -> Result<(), String> {
        if !self.config.permissions.schedule {
            return Err("schedule tasks are not permitted".to_string());
        }

        if interval_ms == 0 {
            return Err("interval must be greater than 0".to_string());
        }

        self.schedule_tx
            .send(ScheduleCmd::Register {
                plugin: self.plugin_name.clone(),
                name,
                interval_ms,
            })
            .map_err(|_| "schedule worker dropped".to_string())
    }

    async fn cancel_task(&mut self, name: String) -> Result<(), String> {
        if !self.config.permissions.schedule {
            return Err("schedule tasks are not permitted".to_string());
        }

        self.schedule_tx
            .send(ScheduleCmd::Unregister {
                plugin: self.plugin_name.clone(),
                name,
            })
            .map_err(|_| "schedule worker dropped".to_string())
    }

    async fn application_id(&mut self) -> Option<String> {
        let id = self.application_id.load(Ordering::Relaxed);
        if id == 0 { None } else { Some(id.to_string()) }
    }

    async fn update_presence(
        &mut self,
        status: String,
        activity_type: u8,
        activity_name: String,
    ) -> Result<(), String> {
        let kind = match activity_type {
            1 => ActivityType::Streaming,
            2 => ActivityType::Listening,
            3 => ActivityType::Watching,
            4 => ActivityType::Custom,
            5 => ActivityType::Competing,
            _ => ActivityType::Playing,
        };
        let activity = Activity {
            application_id: None,
            assets: None,
            buttons: Vec::new(),
            created_at: None,
            details: None,
            emoji: None,
            flags: None,
            id: None,
            instance: None,
            kind,
            name: activity_name,
            party: None,
            secrets: None,
            state: None,
            timestamps: None,
            url: None,
        };
        let status = match status.to_lowercase().as_str() {
            "dnd" | "donotdisturb" => Status::DoNotDisturb,
            "idle" => Status::Idle,
            "invisible" => Status::Invisible,
            "offline" => Status::Offline,
            _ => Status::Online,
        };
        let command = UpdatePresence::new(vec![activity], false, None::<u64>, status)
            .map_err(|e| e.to_string())?;

        let senders = self.shard_senders.lock().await;
        if senders.is_empty() {
            return Err("no shard senders available".to_string());
        }
        for sender in senders.iter() {
            sender.command(&command).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn update_voice_state(
        &mut self,
        guild_id: u64,
        channel_id: Option<u64>,
        self_mute: bool,
        self_deaf: bool,
    ) -> Result<(), String> {
        let shard_count = self.shard_count.load(Ordering::Relaxed);
        if shard_count == 0 {
            return Err("shards not ready".to_string());
        }
        let shard_id = ((guild_id >> 22) % shard_count) as usize;
        let senders = self.shard_senders.lock().await;
        let sender = senders
            .get(shard_id)
            .ok_or_else(|| "shard sender not found".to_string())?;

        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;
        let channel_id = channel_id
            .and_then(NonZeroU64::new)
            .map(|nz| Id::<ChannelMarker>::new(nz.get()));
        let command = UpdateVoiceState::new(guild_id, channel_id, self_deaf, self_mute);
        sender.command(&command).map_err(|e| e.to_string())
    }

    async fn request_guild_members(&mut self, guild_id: u64) -> Result<(), String> {
        let shard_count = self.shard_count.load(Ordering::Relaxed);
        if shard_count == 0 {
            return Err("shards not ready".to_string());
        }
        let shard_id = ((guild_id >> 22) % shard_count) as usize;
        let senders = self.shard_senders.lock().await;
        let sender = senders
            .get(shard_id)
            .ok_or_else(|| "shard sender not found".to_string())?;

        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;
        let command = RequestGuildMembers::builder(guild_id).query("", None);
        sender.command(&command).map_err(|e| e.to_string())
    }

    async fn join_voice_channel(
        &mut self,
        guild_id: u64,
        channel_id: u64,
        self_mute: bool,
        self_deaf: bool,
    ) -> Result<(), String> {
        self.update_voice_state(guild_id, Some(channel_id), self_mute, self_deaf)
            .await
    }

    async fn leave_voice_channel(&mut self, guild_id: u64) -> Result<(), String> {
        self.update_voice_state(guild_id, None, false, false).await
    }

    async fn play_audio_url(&mut self, guild_id: u64, url: String) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let mut call = call.lock().await;

        let input: songbird::input::Input =
            songbird::input::HttpRequest::new(self.client.clone(), url).into();
        call.play_input(input);
        Ok(())
    }

    async fn stop_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let mut call = call.lock().await;

        call.stop();
        Ok(())
    }

    async fn pause_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().pause().map_err(|e| e.to_string())
    }

    async fn resume_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().resume().map_err(|e| e.to_string())
    }

    async fn skip_audio(&mut self, guild_id: u64) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        call.queue().skip().map_err(|e| e.to_string())
    }

    async fn set_volume(&mut self, guild_id: u64, volume: f32) -> Result<(), String> {
        let guild_id = NonZeroU64::new(guild_id)
            .map(|nz| Id::<GuildMarker>::new(nz.get()))
            .ok_or_else(|| "invalid guild id".to_string())?;

        let songbird_guard = self.songbird.lock().await;
        let songbird = songbird_guard
            .as_ref()
            .ok_or_else(|| "voice driver not ready".to_string())?;

        let call = songbird
            .get(guild_id)
            .ok_or_else(|| "bot is not in a voice channel".to_string())?;
        let call = call.lock().await;

        let handle = call
            .queue()
            .current()
            .ok_or_else(|| "no active track".to_string())?;

        handle.set_volume(volume).map_err(|e| e.to_string())
    }

    async fn now_ms(&mut self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    async fn log(&mut self, level: String, message: String) {
        match level.to_lowercase().as_str() {
            "error" => tracing::error!("{message}"),
            "warn" => tracing::warn!("{message}"),
            "info" => tracing::info!("{message}"),
            "debug" => tracing::debug!("{message}"),
            "trace" => tracing::trace!("{message}"),
            _ => tracing::info!("{message}"),
        }
    }

    async fn kv_get(&mut self, key: String) -> Option<Vec<u8>> {
        if !self.config.permissions.kv {
            return None;
        }

        match self.kv.get(&self.plugin_name, &key).await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("kv_get failed for {}/{key}: {e}", self.plugin_name);
                None
            }
        }
    }

    async fn kv_set(&mut self, key: String, value: Vec<u8>) {
        if !self.config.permissions.kv {
            return;
        }

        if let Err(e) = self.kv.set(self.plugin_name.clone(), key, value).await {
            tracing::error!("kv_set failed for {}: {e}", self.plugin_name);
        }
    }

    async fn fs_read(&mut self, path: String) -> Result<Vec<u8>, String> {
        if !self.config.permissions.fs_read {
            return Err("fs read is not permitted".to_string());
        }

        let workspace = self.workspace.clone();
        tokio::task::spawn_blocking(move || workspace_read(&workspace, &path))
            .await
            .map_err(|e| e.to_string())?
    }

    async fn fs_write(&mut self, path: String, content: Vec<u8>) -> Result<(), String> {
        if !self.config.permissions.fs_write {
            return Err("fs write is not permitted".to_string());
        }

        let workspace = self.workspace.clone();
        tokio::task::spawn_blocking(move || workspace_write(&workspace, &path, &content))
            .await
            .map_err(|e| e.to_string())?
    }

    async fn bus_subscribe(&mut self, topics: Vec<String>) -> Result<(), String> {
        if !self.config.permissions.bus {
            return Err("bus access is not permitted".to_string());
        }

        self.bus_subscriptions
            .lock()
            .await
            .entry(self.plugin_name.clone())
            .or_default()
            .extend(topics);
        Ok(())
    }

    async fn bus_publish(&mut self, topic: String, payload: Vec<u8>) -> Result<(), String> {
        if !self.config.permissions.bus {
            return Err("bus access is not permitted".to_string());
        }

        let subscriptions = self.bus_subscriptions.lock().await;
        let mut queue = self.bus_queue.lock().await;
        for (subscriber, subscribed_topics) in subscriptions.iter() {
            if subscriber == &self.plugin_name {
                continue;
            }
            if subscribed_topics.contains(&topic) {
                queue
                    .entry(subscriber.clone())
                    .or_default()
                    .push_back(BusMessage {
                        topic: topic.clone(),
                        payload: payload.clone(),
                    });
            }
        }
        Ok(())
    }
}
