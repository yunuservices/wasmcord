use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Default, Serialize, Deserialize)]
struct Inner {
    #[serde(flatten)]
    scopes: HashMap<String, HashMap<String, Vec<u8>>>,
}

#[derive(Clone)]
pub struct KvStore {
    path: PathBuf,
    inner: Arc<Mutex<Inner>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self::with_path(kv_path())
    }

    pub fn with_path(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(Inner::default())),
        }
    }

    pub fn load_or_default(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let inner = if path.exists() {
            let bytes = std::fs::read(path)
                .with_context(|| format!("failed to read kv store at {}", path.display()))?;
            let inner: Inner = serde_json::from_slice(&bytes)
                .with_context(|| format!("kv store at {} is not valid json", path.display()))?;
            inner
        } else {
            Inner::default()
        };

        Ok(Self {
            path: path.to_path_buf(),
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn get(&self, scope: &str, key: &str) -> Option<Vec<u8>> {
        let inner = self.inner.lock().ok()?;
        inner.scopes.get(scope)?.get(key).cloned()
    }

    pub fn set(&self, scope: String, key: String, value: Vec<u8>) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.scopes.entry(scope).or_default().insert(key, value);
        }
    }

    pub fn save(&self) -> Result<()> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| anyhow::anyhow!("kv store mutex poisoned: {e}"))?;

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create kv store dir {}", parent.display()))?;
        }

        let bytes = serde_json::to_vec_pretty(&*inner).context("failed to serialize kv store")?;
        std::fs::write(&self.path, bytes)
            .with_context(|| format!("failed to write kv store to {}", self.path.display()))?;

        Ok(())
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

pub fn kv_path() -> PathBuf {
    std::env::var("KV_STORE_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./kv-store.json"))
}
