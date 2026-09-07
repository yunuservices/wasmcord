use std::collections::{HashMap, HashSet, VecDeque};

use semver::{Version, VersionReq};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use songbird::Songbird;
use tokio::sync::Mutex as AsyncMutex;
use tracing::info;
use twilight_gateway::MessageSender;
use twilight_model::gateway::event::Event as GatewayEvent;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};

use super::config::{PluginConfig, PluginManifest};
use super::host::{BusMessage, HostContext};
use super::kv::KvStore;
use super::plugin;
use super::workspace::workspace_path;

const PLUGIN_CALL_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const _: () = assert!(
    HTTP_TIMEOUT.as_secs() < PLUGIN_CALL_TIMEOUT.as_secs(),
    "a host http call must be able to time out inside the plugin call budget"
);
const PLUGIN_FUEL: u64 = 50_000_000;
const FUEL_ASYNC_YIELD_INTERVAL: u64 = 10_000;
const PLUGIN_HOSTCALL_FUEL: usize = 1_000_000;
const PLUGIN_ABI_VERSION: u64 = 2;
pub(crate) const DISCORD_RATE_LIMIT_MAX_CONCURRENT: usize = 5;
pub(crate) const DISCORD_REQUEST_MAX_RETRIES: u32 = 3;

#[derive(Clone, Debug)]
pub(crate) enum ScheduleCmd {
    Register {
        plugin: String,
        name: String,
        interval_ms: u64,
    },
    Unregister {
        plugin: String,
        name: String,
    },
}

#[allow(dead_code)]
pub(crate) struct LoadedPlugin {
    component: Arc<Component>,
    config: PluginConfig,
    version: Version,
    provides: Vec<String>,
}

const PLUGIN_FAILURE_THRESHOLD: u32 = 5;

type ScheduleMap = Arc<AsyncMutex<HashMap<(String, String), tokio::task::JoinHandle<()>>>>;

#[derive(Clone)]
pub struct PluginManager {
    plugins: Arc<AsyncMutex<HashMap<String, LoadedPlugin>>>,
    engine: Arc<Engine>,
    gateway_ping_ms: Arc<AtomicU64>,
    application_id: Arc<AtomicU64>,
    shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
    shard_count: Arc<AtomicU64>,
    songbird: Arc<AsyncMutex<Option<Songbird>>>,
    plugin_failures: Arc<AsyncMutex<HashMap<String, u32>>>,
    bus_subscriptions: Arc<AsyncMutex<HashMap<String, HashSet<String>>>>,
    bus_queue: Arc<AsyncMutex<HashMap<String, VecDeque<BusMessage>>>>,
    schedules: ScheduleMap,
    schedule_tx: tokio::sync::mpsc::UnboundedSender<ScheduleCmd>,
    kv: KvStore,
}

pub fn plugin_dir() -> PathBuf {
    std::env::var("PLUGIN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./plugins"))
}

async fn load_manifest(wasm_path: &Path) -> PluginManifest {
    let manifest_path = wasm_path.with_extension("toml");

    if !manifest_path.exists() {
        return PluginManifest::default();
    }

    match tokio::fs::read_to_string(&manifest_path).await {
        Ok(text) => toml::from_str(&text).unwrap_or_else(|err| {
            tracing::warn!(
                "Failed to parse plugin manifest at {}: {err}",
                manifest_path.display()
            );
            PluginManifest::default()
        }),
        Err(err) => {
            tracing::warn!(
                "Failed to read plugin manifest at {}: {err}",
                manifest_path.display()
            );
            PluginManifest::default()
        }
    }
}

fn configure_store(store: &mut Store<HostContext>) -> Result<()> {
    store.set_fuel(PLUGIN_FUEL)?;
    store.fuel_async_yield_interval(Some(FUEL_ASYNC_YIELD_INTERVAL))?;
    store.set_hostcall_fuel(PLUGIN_HOSTCALL_FUEL);
    store.limiter(|state| &mut state.limiter);
    Ok(())
}

fn create_linker(engine: &Engine) -> Result<Linker<HostContext>> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
    plugin::PluginWorld::add_to_linker::<HostContext, HostContext>(&mut linker, |s| s)?;
    Ok(linker)
}

impl PluginManager {
    pub fn new(engine: &Engine) -> Result<Self> {
        let (schedule_tx, mut schedule_rx) = tokio::sync::mpsc::unbounded_channel();
        let manager = Self {
            plugins: Arc::new(AsyncMutex::new(HashMap::new())),
            engine: Arc::new(engine.clone()),
            gateway_ping_ms: Arc::new(AtomicU64::new(0)),
            application_id: Arc::new(AtomicU64::new(0)),
            shard_senders: Arc::new(AsyncMutex::new(Vec::new())),
            shard_count: Arc::new(AtomicU64::new(0)),
            songbird: Arc::new(AsyncMutex::new(None)),
            plugin_failures: Arc::new(AsyncMutex::new(HashMap::new())),
            bus_subscriptions: Arc::new(AsyncMutex::new(HashMap::new())),
            bus_queue: Arc::new(AsyncMutex::new(HashMap::new())),
            schedules: Arc::new(AsyncMutex::new(HashMap::new())),
            schedule_tx,
            kv: KvStore::with_path(super::kv::kv_path())?,
        };

        let manager_for_worker = manager.clone();
        tokio::spawn(async move {
            while let Some(cmd) = schedule_rx.recv().await {
                match cmd {
                    ScheduleCmd::Register {
                        plugin,
                        name,
                        interval_ms,
                    } => {
                        let manager = manager_for_worker.clone();
                        let task_name = name.clone();
                        let handle = tokio::spawn(async move {
                            let mut interval =
                                tokio::time::interval(Duration::from_millis(interval_ms));
                            interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {
                                interval.tick().await;
                                let payload = serde_json::json!({ "name": task_name })
                                    .to_string()
                                    .into_bytes();
                                manager.dispatch_event("SCHEDULE", payload, 0, 0).await;
                            }
                        });
                        manager_for_worker
                            .schedules
                            .lock()
                            .await
                            .insert((plugin, name), handle);
                    }
                    ScheduleCmd::Unregister { plugin, name } => {
                        if let Some(handle) = manager_for_worker
                            .schedules
                            .lock()
                            .await
                            .remove(&(plugin, name))
                        {
                            handle.abort();
                        }
                    }
                }
            }
        });

        Ok(manager)
    }

    pub fn set_gateway_ping_ms(&self, ms: u64) {
        self.gateway_ping_ms.store(ms, Ordering::Relaxed);
    }

    pub fn set_application_id(&self, id: u64) {
        self.application_id.store(id, Ordering::Relaxed);
    }

    pub async fn set_shard_senders(&self, senders: Vec<MessageSender>) {
        *self.shard_senders.lock().await = senders;
    }

    pub async fn clone_shard_senders(&self) -> Vec<MessageSender> {
        self.shard_senders.lock().await.clone()
    }

    pub fn set_shard_count(&self, count: u64) {
        self.shard_count.store(count, Ordering::Relaxed);
    }

    pub async fn set_songbird(&self, songbird: Songbird) {
        *self.songbird.lock().await = Some(songbird);
    }

    pub async fn process_voice_event(&self, event: GatewayEvent) {
        if let Some(songbird) = self.songbird.lock().await.as_ref() {
            songbird.process(&event).await;
        }
    }

    pub async fn load_all(&self) -> Result<()> {
        let path = plugin_dir();
        if !path.exists() {
            tokio::fs::create_dir_all(&path).await?;
            info!("Created plugin directory: {}", path.display());
        }

        let mut pending: HashMap<String, (PathBuf, PluginManifest)> = HashMap::new();
        let mut read = tokio::fs::read_dir(&path).await?;
        while let Some(entry) = read.next_entry().await? {
            let wasm_path = entry.path();
            if wasm_path.extension().is_some_and(|e| e == "wasm") {
                let manifest = load_manifest(&wasm_path).await;
                let name = manifest
                    .plugin
                    .name
                    .clone()
                    .unwrap_or_else(|| Self::plugin_name(&wasm_path));
                pending.insert(name, (wasm_path, manifest));
            }
        }

        if let Err(e) = Self::validate_dependencies(&pending) {
            tracing::error!("Plugin dependency validation failed: {e}");
            anyhow::bail!(e);
        }

        let order = Self::resolve_load_order(&pending)?;

        let mut loaded_count = 0;
        for name in order {
            let (wasm_path, manifest) = pending.remove(&name).expect("pending plugin missing");
            match Self::load_one(
                &self.engine,
                Arc::clone(&self.gateway_ping_ms),
                Arc::clone(&self.application_id),
                Arc::clone(&self.shard_senders),
                Arc::clone(&self.shard_count),
                Arc::clone(&self.songbird),
                Arc::clone(&self.bus_subscriptions),
                Arc::clone(&self.bus_queue),
                self.schedule_tx.clone(),
                self.kv.clone(),
                &wasm_path,
                &manifest,
            )
            .await
            {
                Ok((loaded_name, loaded)) => {
                    self.plugins.lock().await.insert(loaded_name, loaded);
                    loaded_count += 1;
                }
                Err(e) => tracing::error!("Failed to load {}: {e}", wasm_path.display()),
            }
        }

        info!(count = loaded_count, "Plugins loaded");
        Ok(())
    }

    fn validate_dependencies(pending: &HashMap<String, (PathBuf, PluginManifest)>) -> Result<()> {
        for (name, (_, manifest)) in pending {
            for (dep_name, spec) in &manifest.dependencies {
                match pending.get(dep_name) {
                    Some((_, dep_manifest)) => {
                        let req = VersionReq::parse(&spec.version).with_context(|| {
                            format!("invalid version requirement for {dep_name} in {name}")
                        })?;
                        if !req.matches(&dep_manifest.plugin.version) {
                            anyhow::bail!(
                                "plugin {name} requires {dep_name} {req}, but found {}",
                                dep_manifest.plugin.version
                            );
                        }
                    }
                    None => {
                        if spec.optional {
                            tracing::warn!(
                                "Plugin {name} optional dependency {dep_name} is not present"
                            );
                        } else {
                            anyhow::bail!("plugin {name} requires missing dependency {dep_name}");
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn resolve_load_order(
        pending: &HashMap<String, (PathBuf, PluginManifest)>,
    ) -> Result<Vec<String>> {
        let mut in_degree: HashMap<String, usize> =
            pending.keys().map(|k| (k.clone(), 0)).collect();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

        for (name, (_, manifest)) in pending {
            for dep_name in manifest.dependencies.keys() {
                if pending.contains_key(dep_name) {
                    dependents
                        .entry(dep_name.clone())
                        .or_default()
                        .push(name.clone());
                    *in_degree.get_mut(name).unwrap() += 1;
                }
            }
        }

        let mut queue: VecDeque<String> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(name, _)| name.clone())
            .collect();
        let mut order = Vec::new();

        while let Some(name) = queue.pop_front() {
            order.push(name.clone());
            for dependent in dependents.get(&name).cloned().unwrap_or_default() {
                let degree = in_degree.get_mut(&dependent).unwrap();
                *degree -= 1;
                if *degree == 0 {
                    queue.push_back(dependent);
                }
            }
        }

        if order.len() != pending.len() {
            anyhow::bail!("circular plugin dependency detected");
        }

        Ok(order)
    }

    pub fn plugin_name(path: &Path) -> String {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn load_one(
        engine: &Engine,
        gateway_ping_ms: Arc<AtomicU64>,
        application_id: Arc<AtomicU64>,
        shard_senders: Arc<AsyncMutex<Vec<MessageSender>>>,
        shard_count: Arc<AtomicU64>,
        songbird: Arc<AsyncMutex<Option<Songbird>>>,
        bus_subscriptions: Arc<AsyncMutex<HashMap<String, HashSet<String>>>>,
        bus_queue: Arc<AsyncMutex<HashMap<String, VecDeque<BusMessage>>>>,
        schedule_tx: tokio::sync::mpsc::UnboundedSender<ScheduleCmd>,
        kv: KvStore,
        wasm_path: &Path,
        manifest: &PluginManifest,
    ) -> Result<(String, LoadedPlugin)> {
        let bytes = tokio::fs::read(wasm_path).await?;
        let name = manifest
            .plugin
            .name
            .clone()
            .unwrap_or_else(|| Self::plugin_name(wasm_path));

        if manifest.plugin.abi_version != 0 && manifest.plugin.abi_version != PLUGIN_ABI_VERSION {
            anyhow::bail!(
                "plugin {name} requires ABI version {}, host supports {}",
                manifest.plugin.abi_version,
                PLUGIN_ABI_VERSION
            );
        }

        let component = Component::new(engine, &bytes)?;
        let workspace = workspace_path(&name);
        let config = PluginConfig {
            permissions: manifest.permissions.clone(),
            limits: manifest.limits,
        };
        tokio::fs::create_dir_all(&workspace).await?;

        let version = manifest.plugin.version.clone();
        let provides = manifest.provides.clone();

        let mut store = Store::new(
            engine,
            HostContext::new(
                gateway_ping_ms,
                application_id,
                shard_senders,
                shard_count,
                songbird,
                bus_subscriptions,
                bus_queue,
                schedule_tx,
                name.clone(),
                kv.clone(),
                workspace.clone(),
                config.clone(),
            ),
        );
        configure_store(&mut store)?;

        let linker = create_linker(engine)?;
        let instance =
            plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await?;

        match instance
            .ynsrvcs_plugins_plugin()
            .call_initialize(&mut store, None)
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => anyhow::bail!("plugin initialization failed: {err}"),
            Err(err) => anyhow::bail!("plugin initialization trapped: {err}"),
        }

        Ok((
            name,
            LoadedPlugin {
                component: Arc::new(component),
                config,
                version,
                provides,
            },
        ))
    }

    pub async fn load(&self, wasm_path: &Path) -> Result<String> {
        let manifest = load_manifest(wasm_path).await;
        let (name, loaded) = Self::load_one(
            &self.engine,
            Arc::clone(&self.gateway_ping_ms),
            Arc::clone(&self.application_id),
            Arc::clone(&self.shard_senders),
            Arc::clone(&self.shard_count),
            Arc::clone(&self.songbird),
            Arc::clone(&self.bus_subscriptions),
            Arc::clone(&self.bus_queue),
            self.schedule_tx.clone(),
            self.kv.clone(),
            wasm_path,
            &manifest,
        )
        .await?;
        self.plugins.lock().await.insert(name.clone(), loaded);
        Ok(name)
    }

    /// Safely reload a plugin: load the new version first, then shutdown the
    /// old version. If the new version fails to load, the old plugin stays in
    /// service.
    pub async fn reload_plugin(&self, wasm_path: &Path) -> Result<String> {
        let manifest = load_manifest(wasm_path).await;
        let name = manifest
            .plugin
            .name
            .clone()
            .unwrap_or_else(|| Self::plugin_name(wasm_path));
        let (loaded_name, loaded) = Self::load_one(
            &self.engine,
            Arc::clone(&self.gateway_ping_ms),
            Arc::clone(&self.application_id),
            Arc::clone(&self.shard_senders),
            Arc::clone(&self.shard_count),
            Arc::clone(&self.songbird),
            Arc::clone(&self.bus_subscriptions),
            Arc::clone(&self.bus_queue),
            self.schedule_tx.clone(),
            self.kv.clone(),
            wasm_path,
            &manifest,
        )
        .await?;

        if loaded_name != name {
            tracing::warn!(
                "Reload path {} produced plugin name {}, expected {}",
                wasm_path.display(),
                loaded_name,
                name
            );
        }

        if self.is_loaded(&name).await {
            self.unload(&name).await;
        }

        self.plugins.lock().await.insert(name.clone(), loaded);
        self.plugin_failures.lock().await.remove(&name);
        Ok(name)
    }

    pub async fn unload(&self, name: &str) {
        self.cancel_all_schedules(name).await;

        let maybe_loaded = {
            let plugins = self.plugins.lock().await;
            plugins
                .get(name)
                .map(|loaded| (Arc::clone(&loaded.component), loaded.config.clone()))
        };

        if let Some((component, config)) = maybe_loaded {
            let mut store = Store::new(
                &self.engine,
                HostContext::new(
                    Arc::clone(&self.gateway_ping_ms),
                    Arc::clone(&self.application_id),
                    Arc::clone(&self.shard_senders),
                    Arc::clone(&self.shard_count),
                    Arc::clone(&self.songbird),
                    Arc::clone(&self.bus_subscriptions),
                    Arc::clone(&self.bus_queue),
                    self.schedule_tx.clone(),
                    name.to_string(),
                    self.kv.clone(),
                    workspace_path(name),
                    config,
                ),
            );
            if let Err(err) = configure_store(&mut store) {
                tracing::error!("Failed to configure store for {name} shutdown: {err}");
            }
            let linker = match create_linker(&self.engine) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to create linker for {name} shutdown: {e}");
                    self.plugins.lock().await.remove(name);
                    return;
                }
            };

            match plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await {
                Ok(instance) => {
                    if let Err(e) = instance
                        .ynsrvcs_plugins_plugin()
                        .call_shutdown(&mut store)
                        .await
                    {
                        tracing::warn!("Shutdown trap for {name}: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to instantiate {name} for shutdown: {e}");
                }
            }
        }

        self.plugins.lock().await.remove(name);
        info!("Plugin unloaded: {name}");
    }

    pub async fn unload_all(&self) {
        let names: Vec<String> = {
            let plugins = self.plugins.lock().await;
            plugins.keys().cloned().collect()
        };
        for name in names {
            self.unload(&name).await;
        }
    }

    pub async fn save_kv(&self) -> Result<()> {
        self.kv.save().await
    }

    pub async fn unload_by_path(&self, wasm_path: &Path) {
        let name = Self::plugin_name(wasm_path);
        self.unload(&name).await;
    }

    pub async fn is_loaded(&self, name: &str) -> bool {
        self.plugins.lock().await.contains_key(name)
    }

    async fn cancel_all_schedules(&self, plugin_name: &str) {
        let mut schedules = self.schedules.lock().await;
        let keys: Vec<(String, String)> = schedules
            .keys()
            .filter(|(plugin, _)| plugin == plugin_name)
            .cloned()
            .collect();
        for key in keys {
            if let Some(handle) = schedules.remove(&key) {
                handle.abort();
            }
        }
    }

    pub async fn loaded_names(&self) -> Vec<String> {
        self.plugins.lock().await.keys().cloned().collect()
    }

    pub async fn dispatch_event(
        &self,
        event_type: &str,
        payload: Vec<u8>,
        guild_id: u64,
        channel_id: u64,
    ) {
        let plugins = {
            let guard = self.plugins.lock().await;
            guard
                .iter()
                .map(|(name, loaded)| {
                    (
                        name.clone(),
                        Arc::clone(&loaded.component),
                        loaded.config.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let failures = Arc::clone(&self.plugin_failures);
        let manager = self.clone();

        for (name, component, config) in plugins {
            let engine = Arc::clone(&self.engine);
            let gateway_ping_ms = Arc::clone(&self.gateway_ping_ms);
            let application_id = Arc::clone(&self.application_id);
            let shard_senders = Arc::clone(&self.shard_senders);
            let shard_count = Arc::clone(&self.shard_count);
            let songbird = Arc::clone(&self.songbird);
            let kv = self.kv.clone();
            let kv_for_save = kv.clone();
            let workspace = workspace_path(&name);
            let event_type = event_type.to_string();
            let payload = payload.clone();
            let failures = Arc::clone(&failures);
            let manager = manager.clone();

            let handle = async move {
                let mut store = Store::new(
                    &engine,
                    HostContext::new(
                        gateway_ping_ms,
                        application_id,
                        shard_senders,
                        shard_count,
                        songbird,
                        Arc::clone(&manager.bus_subscriptions),
                        Arc::clone(&manager.bus_queue),
                        manager.schedule_tx.clone(),
                        name.clone(),
                        kv,
                        workspace,
                        config,
                    ),
                );
                if let Err(err) = configure_store(&mut store) {
                    tracing::error!("Failed to configure store for {name}: {err}");
                    return;
                }
                let linker = match create_linker(&engine) {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::error!("Failed to create linker for {name}: {e}");
                        return;
                    }
                };

                let instance =
                    match plugin::PluginWorld::instantiate_async(&mut store, &component, &linker)
                        .await
                    {
                        Ok(i) => i,
                        Err(e) => {
                            tracing::error!("Failed to instantiate {name} for {event_type}: {e}");
                            return;
                        }
                    };

                let guest = instance.ynsrvcs_plugins_plugin();
                let fut = guest.call_handle_event(
                    &mut store,
                    &event_type,
                    &payload,
                    guild_id,
                    channel_id,
                );

                let failed = match tokio::time::timeout(PLUGIN_CALL_TIMEOUT, fut).await {
                    Ok(Ok(Ok(()))) => {
                        failures.lock().await.remove(&name);
                        false
                    }
                    Ok(Ok(Err(err))) => {
                        tracing::error!("Plugin {name} error handling {event_type}: {err}");
                        true
                    }
                    Ok(Err(err)) => {
                        tracing::error!("Plugin {name} trapped handling {event_type}: {err}");
                        true
                    }
                    Err(_) => {
                        tracing::error!("Plugin {name} timed out handling {event_type}");
                        true
                    }
                };

                if failed {
                    let mut map = failures.lock().await;
                    let count = map.entry(name.clone()).or_insert(0);
                    *count += 1;
                    if *count >= PLUGIN_FAILURE_THRESHOLD {
                        tracing::error!("Plugin {name} exceeded failure threshold; unloading");
                        drop(map);
                        manager.unload(&name).await;
                    }
                }

                if let Err(err) = kv_for_save.save().await {
                    tracing::error!("Failed to persist KV after {event_type} for {name}: {err}");
                }
            };

            handle.await;
        }

        self.flush_bus_events().await;
    }

    async fn flush_bus_events(&self) {
        let work: Vec<(String, Vec<BusMessage>, Arc<Component>, PluginConfig)> = {
            let mut queue = self.bus_queue.lock().await;
            let plugins = self.plugins.lock().await;
            let subscriptions = self.bus_subscriptions.lock().await;

            subscriptions
                .keys()
                .filter_map(|name| {
                    let messages: Vec<BusMessage> = queue.get_mut(name)?.drain(..).collect();
                    if messages.is_empty() {
                        return None;
                    }
                    let loaded = plugins.get(name)?;
                    Some((
                        name.clone(),
                        messages,
                        Arc::clone(&loaded.component),
                        loaded.config.clone(),
                    ))
                })
                .collect()
        };

        for (name, messages, component, config) in work {
            let engine = Arc::clone(&self.engine);
            let gateway_ping_ms = Arc::clone(&self.gateway_ping_ms);
            let application_id = Arc::clone(&self.application_id);
            let shard_senders = Arc::clone(&self.shard_senders);
            let shard_count = Arc::clone(&self.shard_count);
            let songbird = Arc::clone(&self.songbird);
            let bus_subscriptions = Arc::clone(&self.bus_subscriptions);
            let bus_queue = Arc::clone(&self.bus_queue);
            let kv = self.kv.clone();
            let workspace = workspace_path(&name);

            let mut store = Store::new(
                &engine,
                HostContext::new(
                    gateway_ping_ms,
                    application_id,
                    shard_senders,
                    shard_count,
                    songbird,
                    bus_subscriptions,
                    bus_queue,
                    self.schedule_tx.clone(),
                    name.clone(),
                    kv,
                    workspace,
                    config,
                ),
            );
            if let Err(err) = configure_store(&mut store) {
                tracing::error!("Failed to configure store for {name} bus event: {err}");
                continue;
            }
            let linker = match create_linker(&engine) {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!("Failed to create linker for {name} bus event: {e}");
                    continue;
                }
            };

            let instance =
                match plugin::PluginWorld::instantiate_async(&mut store, &component, &linker).await
                {
                    Ok(i) => i,
                    Err(e) => {
                        tracing::error!("Failed to instantiate {name} for bus event: {e}");
                        continue;
                    }
                };

            let guest = instance.ynsrvcs_plugins_plugin();
            for BusMessage { topic, payload } in messages {
                let fut = guest.call_handle_bus_event(&mut store, &topic, &payload);
                if let Err(e) = tokio::time::timeout(PLUGIN_CALL_TIMEOUT, fut).await {
                    tracing::error!("Bus event {topic} for {name} timed out: {e}");
                }
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
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

        let wasm_path = ensure_ping_wasm()?;
        let engine = crate::wasm::plugin::create_engine()?;
        let manifest = PluginManifest::default();
        let (name, _) = PluginManager::load_one(
            &engine,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AsyncMutex::new(Vec::new())),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AsyncMutex::new(None)),
            Arc::new(AsyncMutex::new(HashMap::new())),
            Arc::new(AsyncMutex::new(HashMap::new())),
            tokio::sync::mpsc::unbounded_channel().0,
            KvStore::with_path(std::env::temp_dir().join("ynsrvcs-test-kv"))?,
            &wasm_path,
            &manifest,
        )
        .await?;
        assert_eq!(name, "ping");

        Ok(())
    }
}
