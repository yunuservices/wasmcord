use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use twilight_http::Client as HttpClient;
use twilight_model::id::{Id, marker::ChannelMarker};
use wasmtime::component::Component;
use wasmtime::Engine;
use wasmtime::Store;
use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView, WasiCtxView};

use super::plugin;

pub type SharedHttp = Arc<HttpClient>;

pub struct HostContext {
    wasi: WasiCtx,
    table: ResourceTable,
    http: SharedHttp,
}

impl HostContext {
    pub fn new(http: SharedHttp) -> Self {
        Self {
            wasi: WasiCtxBuilder::new().build(),
            table: ResourceTable::default(),
            http,
        }
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
    fn send_message(&mut self, channel_id: u64, content: String) -> Result<(), String> {
        let http = Arc::clone(&self.http);
        tokio::spawn(async move {
            if let Err(e) = http
                .create_message(Id::<ChannelMarker>::new(channel_id))
                .content(&content)
                .await
            {
                tracing::error!(?e, "plugin send_message failed");
            }
        });
        Ok(())
    }

    fn send_interaction_response(
        &mut self,
        _interaction_id: u64,
        _token: String,
        _content: String,
    ) -> Result<(), String> {
        Err("interaction responses not yet implemented".into())
    }

    fn http_request(
        &mut self,
        url: String,
        method: String,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        tokio::task::block_in_place(|| {
            let agent = ureq::agent();
            let http_req = http::Request::builder()
                .method(method.as_str())
                .uri(url.as_str())
                .body(body)
                .map_err(|e| e.to_string())?;
            let resp = agent.run(http_req).map_err(|e| e.to_string())?;
            resp.into_body().read_to_vec().map_err(|e| e.to_string())
        })
    }

    fn log(&mut self, level: String, message: String) {
        match level.to_lowercase().as_str() {
            "error" => tracing::error!("{message}"),
            "warn" => tracing::warn!("{message}"),
            "info" => tracing::info!("{message}"),
            "debug" => tracing::debug!("{message}"),
            "trace" => tracing::trace!("{message}"),
            _ => tracing::info!("{message}"),
        }
    }
}

pub(crate) struct LoadedPlugin {
    world: plugin::PluginWorld,
    store: Store<HostContext>,
    commands: Vec<plugin::exports::ynsrvcs::plugins::plugin::Command>,
}

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<Mutex<HashMap<String, LoadedPlugin>>>,
    engine: Arc<Engine>,
    http: SharedHttp,
}

pub fn plugin_dir() -> String {
    std::env::var("PLUGIN_DIR").unwrap_or_else(|_| "./plugins".into())
}

impl PluginManager {
    pub fn new(engine: &Engine, http: SharedHttp) -> Result<Self> {
        Ok(Self {
            plugins: Arc::new(Mutex::new(HashMap::new())),
            engine: Arc::new(engine.clone()),
            http,
        })
    }

    pub async fn load_all(&self) -> Result<()> {
        let dir = plugin_dir();
        let path = Path::new(&dir);
        if !path.exists() {
            tokio::fs::create_dir_all(path).await?;
            info!("Created plugin directory: {dir}");
        }

        let mut entries = Vec::new();
        let mut read = tokio::fs::read_dir(path).await?;
        while let Some(entry) = read.next_entry().await? {
            let p = entry.path();
            if p.extension().is_some_and(|e| e == "wasm") {
                entries.push(p);
            }
        }

        let mut loaded_plugins = Vec::new();
        for wasm_path in &entries {
            match Self::load_one(&self.engine, &self.http, wasm_path).await {
                Ok((name, loaded)) => {
                    loaded_plugins.push((name, loaded));
                }
                Err(e) => {
                    tracing::error!("Failed to load {}: {e}", wasm_path.display());
                }
            }
        }

        let mut plugins = self.plugins.lock().await;
        for (name, loaded) in loaded_plugins {
            plugins.insert(name, loaded);
        }

        info!(count = plugins.len(), "Plugins loaded");
        Ok(())
    }

    pub fn plugin_name(path: &Path) -> String {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    pub(crate) async fn load_one(
        engine: &Engine,
        http: &SharedHttp,
        wasm_path: &Path,
    ) -> Result<(String, LoadedPlugin)> {
        let bytes = tokio::fs::read(wasm_path).await?;
        let name = Self::plugin_name(wasm_path);

        let component = Component::new(engine, &bytes)?;
        let mut linker = wasmtime::component::Linker::new(engine);

        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;

        plugin::PluginWorld::add_to_linker::<HostContext, HostContext>(
            &mut linker,
            |s: &mut HostContext| s,
        )?;

        let mut store = Store::new(engine, HostContext::new(Arc::clone(http)));
        let world = plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await?;

        let commands = world.ynsrvcs_plugins_plugin().call_init(&mut store)?;
        if !commands.is_empty() {
            info!("Plugin {name} provides commands: {commands:?}");
        }

        Ok((
            name,
            LoadedPlugin {
                world,
                store,
                commands,
            },
        ))
    }

    pub async fn load(&self, wasm_path: &Path) -> Result<String> {
        let (name, loaded) = Self::load_one(&self.engine, &self.http, wasm_path).await?;
        self.plugins.lock().await.insert(name.clone(), loaded);
        Ok(name)
    }

    pub async fn unload(&self, name: &str) {
        self.plugins.lock().await.remove(name);
        info!("Plugin unloaded: {name}");
    }

    pub async fn unload_by_path(&self, wasm_path: &Path) {
        let name = Self::plugin_name(wasm_path);
        self.unload(&name).await;
    }

    pub async fn is_loaded(&self, name: &str) -> bool {
        self.plugins.lock().await.contains_key(name)
    }

    pub async fn loaded_names(&self) -> Vec<String> {
        self.plugins.lock().await.keys().cloned().collect()
    }

    pub async fn list_commands(&self) -> Vec<plugin::exports::ynsrvcs::plugins::plugin::Command> {
        self.plugins
            .lock()
            .await
            .values()
            .flat_map(|p| p.commands.clone())
            .collect()
    }

    pub async fn dispatch_event(
        &self,
        event_type: &str,
        payload: Vec<u8>,
        guild_id: u64,
        channel_id: u64,
    ) {
        use plugin::exports::ynsrvcs::plugins::plugin::EventType;

        let et = match event_type {
            "ready" => EventType::Ready,
            "message-create" => EventType::MessageCreate,
            "interaction-create" => EventType::InteractionCreate,
            "guild-member-add" => EventType::GuildMemberAdd,
            "guild-member-remove" => EventType::GuildMemberRemove,
            "reaction-add" => EventType::ReactionAdd,
            "voice-state-update" => EventType::VoiceStateUpdate,
            _ => return,
        };

        let mut plugins = self.plugins.lock().await;
        for (_name, loaded) in plugins.iter_mut() {
            let guest = loaded.world.ynsrvcs_plugins_plugin();
            if let Err(e) = guest.call_handle_event(
                &mut loaded.store,
                et,
                &payload,
                guild_id,
                channel_id,
            ) {
                tracing::error!("Plugin {_name} error handling {event_type}: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_ping_wasm() -> Result<std::path::PathBuf> {
        let root = std::env::var("CARGO_MANIFEST_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_default();
        let wasm_path = root.join("plugins").join("ping.wasm");
        if wasm_path.exists() {
            return Ok(wasm_path);
        }

        let plugin_dir = root.join("example-plugin");
        let output = std::process::Command::new("cargo")
            .args([
                "build",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
                plugin_dir.join("Cargo.toml").to_str().unwrap(),
            ])
            .output()
            .expect("failed to build example-plugin");

        if !output.status.success() {
            panic!(
                "example-plugin build failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let artifact = plugin_dir
            .join("target")
            .join("wasm32-wasip2")
            .join("debug")
            .join("ping_plugin.wasm");
        if !artifact.exists() {
            panic!("expected wasm artifact at {}", artifact.display());
        }

        std::fs::create_dir_all(wasm_path.parent().unwrap())?;
        std::fs::copy(&artifact, &wasm_path)?;
        Ok(wasm_path)
    }

    #[tokio::test]
    async fn test_load_ping_plugin() -> Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("info")
            .try_init();

        let wasm_path = ensure_ping_wasm()?;
        let engine = crate::wasm::plugin::create_engine()?;
        let http = Arc::new(HttpClient::new("fake-token".to_string()));
        let (name, mut loaded) = PluginManager::load_one(
            &engine,
            &http,
            &wasm_path,
        )
        .await?;

        assert_eq!(name, "ping");

        let guest = loaded.world.ynsrvcs_plugins_plugin();
        let cmds = guest.call_init(&mut loaded.store)?;
        tracing::info!("Plugin commands: {cmds:?}");
        assert_eq!(cmds.len(), 1);
        assert_eq!(cmds[0].name, "ping");

        guest.call_handle_event(
            &mut loaded.store,
            plugin::exports::ynsrvcs::plugins::plugin::EventType::MessageCreate,
            b"hello",
            0,
            0,
        )?;

        Ok(())
    }
}
