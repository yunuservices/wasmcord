use semver::Version;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PluginPermissions {
    #[serde(default)]
    pub http: bool,
    #[serde(default)]
    pub http_allowed_hosts: Vec<String>,
    #[serde(default)]
    pub fs_read: bool,
    #[serde(default)]
    pub fs_write: bool,
    #[serde(default)]
    pub env: Vec<String>,
    #[serde(default)]
    pub kv: bool,
    #[serde(default)]
    pub bus: bool,
    #[serde(default)]
    pub schedule: bool,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct PluginLimits {
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_max_instances")]
    pub max_instances: usize,
    #[serde(default = "default_max_tables")]
    pub max_tables: usize,
    #[serde(default = "default_max_memories")]
    pub max_memories: usize,
}

impl Default for PluginLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: default_max_memory_bytes(),
            max_instances: default_max_instances(),
            max_tables: default_max_tables(),
            max_memories: default_max_memories(),
        }
    }
}

fn default_max_memory_bytes() -> usize {
    64 * 1024 * 1024 // 64 MiB
}

fn default_max_instances() -> usize {
    10
}

fn default_max_tables() -> usize {
    10
}

fn default_max_memories() -> usize {
    1
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PluginConfig {
    #[serde(default)]
    pub permissions: PluginPermissions,
    #[serde(default)]
    pub limits: PluginLimits,
}

#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct ManifestPluginInfo {
    pub name: Option<String>,
    #[serde(default = "default_manifest_version")]
    pub version: Version,
    pub description: Option<String>,
    #[serde(default)]
    pub abi_version: u64,
}

impl Default for ManifestPluginInfo {
    fn default() -> Self {
        Self {
            name: None,
            version: default_manifest_version(),
            description: None,
            abi_version: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct PluginManifest {
    #[serde(default)]
    pub plugin: ManifestPluginInfo,
    #[serde(default)]
    pub dependencies: HashMap<String, DependencySpec>,
    #[serde(default)]
    pub provides: Vec<String>,
    #[serde(default)]
    pub permissions: PluginPermissions,
    #[serde(default)]
    pub limits: PluginLimits,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub(crate) struct DependencySpec {
    pub version: String,
    #[serde(default)]
    pub optional: bool,
}

fn default_manifest_version() -> Version {
    Version::new(0, 0, 0)
}

pub struct PluginResourceLimiter {
    limits: PluginLimits,
}

impl PluginResourceLimiter {
    pub(crate) fn new(limits: PluginLimits) -> Self {
        Self { limits }
    }
}

impl wasmtime::ResourceLimiter for PluginResourceLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(desired <= self.limits.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, wasmtime::Error> {
        Ok(true)
    }

    fn instances(&self) -> usize {
        self.limits.max_instances
    }

    fn memories(&self) -> usize {
        self.limits.max_memories
    }

    fn tables(&self) -> usize {
        self.limits.max_tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_reads_the_permissions_section() {
        let manifest: PluginManifest = toml::from_str(
            r#"
[plugin]
name = "sample"
abi_version = 2

[permissions]
http = true
http_allowed_hosts = ["api.example.com"]
env = ["SAMPLE_KEY"]
kv = true
"#,
        )
        .unwrap();

        assert!(manifest.permissions.http);
        assert_eq!(manifest.permissions.http_allowed_hosts, ["api.example.com"]);
        assert_eq!(manifest.permissions.env, ["SAMPLE_KEY"]);
        assert!(manifest.permissions.kv);
        assert!(!manifest.permissions.fs_read);
    }

    #[test]
    fn manifest_without_permissions_grants_nothing() {
        let manifest: PluginManifest = toml::from_str("[plugin]\nname = \"sample\"\n").unwrap();

        assert!(!manifest.permissions.http);
        assert!(!manifest.permissions.kv);
        assert!(manifest.permissions.env.is_empty());
        assert!(manifest.permissions.http_allowed_hosts.is_empty());
    }
}
