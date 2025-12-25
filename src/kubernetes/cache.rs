// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Local cache for discovered Kubernetes resources (CRDs)
//!
//! Caches CRD discovery results to enable fast startup on subsequent runs.
//! Core K8s resources are always loaded from k8s-openapi (instant).

use anyhow::{Context, Result};
use kube::discovery::{ApiCapabilities, ApiResource, Scope};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use super::discovery::{ResourceInfo, ResourceRegistry};

/// Default cache TTL (24 hours)
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Serializable version of ResourceInfo for caching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedResourceInfo {
    // ApiResource fields
    pub group: String,
    pub version: String,
    pub api_version: String,
    pub kind: String,
    pub plural: String,
    // ApiCapabilities fields
    pub scope: String, // "Namespaced" or "Cluster"
    // ResourceInfo fields
    pub table_name: String,
    pub aliases: Vec<String>,
    pub is_core: bool,
}

impl From<&ResourceInfo> for CachedResourceInfo {
    fn from(info: &ResourceInfo) -> Self {
        Self {
            group: info.api_resource.group.clone(),
            version: info.api_resource.version.clone(),
            api_version: info.api_resource.api_version.clone(),
            kind: info.api_resource.kind.clone(),
            plural: info.api_resource.plural.clone(),
            scope: match info.capabilities.scope {
                Scope::Namespaced => "Namespaced".to_string(),
                Scope::Cluster => "Cluster".to_string(),
            },
            table_name: info.table_name.clone(),
            aliases: info.aliases.clone(),
            is_core: info.is_core,
        }
    }
}

impl From<CachedResourceInfo> for ResourceInfo {
    fn from(cached: CachedResourceInfo) -> Self {
        let scope = if cached.scope == "Namespaced" {
            Scope::Namespaced
        } else {
            Scope::Cluster
        };

        Self {
            api_resource: ApiResource {
                group: cached.group.clone(),
                version: cached.version.clone(),
                api_version: cached.api_version,
                kind: cached.kind,
                plural: cached.plural,
            },
            capabilities: ApiCapabilities {
                scope,
                subresources: vec![],
                operations: vec![],
            },
            table_name: cached.table_name,
            aliases: cached.aliases,
            is_core: cached.is_core,
            group: cached.group,
            version: cached.version,
        }
    }
}

/// Cached registry with metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct CachedRegistry {
    /// When this cache was created
    pub created_at: u64, // Unix timestamp
    /// The cached resources (CRDs only, core resources are not cached)
    pub resources: Vec<CachedResourceInfo>,
}

impl CachedRegistry {
    /// Create a new cached registry from discovered resources
    /// Only includes non-core resources (CRDs)
    pub fn from_registry(registry: &ResourceRegistry) -> Self {
        let resources: Vec<CachedResourceInfo> = registry
            .list_tables()
            .into_iter()
            .filter(|info| !info.is_core) // Only cache CRDs
            .map(CachedResourceInfo::from)
            .collect();

        Self {
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            resources,
        }
    }

    /// Check if this cache is still fresh
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.saturating_sub(self.created_at);
        age < ttl.as_secs()
    }

    /// Convert back to ResourceRegistry (CRDs only)
    pub fn to_registry(&self) -> ResourceRegistry {
        let mut registry = ResourceRegistry::new();
        for cached in &self.resources {
            registry.add(cached.clone().into());
        }
        registry
    }
}

/// Resource cache manager
pub struct ResourceCache {
    cache_dir: PathBuf,
    ttl: Duration,
}

impl ResourceCache {
    /// Create a new cache manager
    pub fn new() -> Result<Self> {
        let cache_dir = Self::default_cache_dir()?;
        Ok(Self {
            cache_dir,
            ttl: DEFAULT_CACHE_TTL,
        })
    }

    /// Get the default cache directory
    fn default_cache_dir() -> Result<PathBuf> {
        let cache_base = dirs::cache_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
            .context("Could not determine cache directory")?;
        Ok(cache_base.join("k8sql").join("clusters"))
    }

    /// Get the cache file path for a cluster
    fn cache_path(&self, cluster: &str) -> PathBuf {
        // Sanitize cluster name for filesystem
        let safe_name: String = cluster
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.cache_dir.join(format!("{}.json", safe_name))
    }

    /// Ensure cache directory exists
    fn ensure_cache_dir(&self) -> Result<()> {
        if !self.cache_dir.exists() {
            std::fs::create_dir_all(&self.cache_dir)
                .context("Failed to create cache directory")?;
        }
        Ok(())
    }

    /// Load cached CRDs for a cluster
    /// Returns None if cache doesn't exist or is stale
    pub fn load(&self, cluster: &str) -> Option<CachedRegistry> {
        let path = self.cache_path(cluster);
        if !path.exists() {
            return None;
        }

        let content = std::fs::read_to_string(&path).ok()?;
        let cached: CachedRegistry = serde_json::from_str(&content).ok()?;

        if cached.is_fresh(self.ttl) {
            Some(cached)
        } else {
            // Cache is stale, remove it
            let _ = std::fs::remove_file(&path);
            None
        }
    }

    /// Save CRDs to cache for a cluster
    pub fn save(&self, cluster: &str, registry: &ResourceRegistry) -> Result<()> {
        self.ensure_cache_dir()?;

        let cached = CachedRegistry::from_registry(registry);
        let content = serde_json::to_string_pretty(&cached)
            .context("Failed to serialize cache")?;

        let path = self.cache_path(cluster);
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write cache to {:?}", path))?;

        Ok(())
    }

    /// Clear cache for a specific cluster
    #[allow(dead_code)]
    pub fn clear(&self, cluster: &str) -> Result<()> {
        let path = self.cache_path(cluster);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Clear all cached data
    #[allow(dead_code)]
    pub fn clear_all(&self) -> Result<()> {
        if self.cache_dir.exists() {
            std::fs::remove_dir_all(&self.cache_dir)?;
        }
        Ok(())
    }
}

impl Default for ResourceCache {
    fn default() -> Self {
        Self::new().expect("Failed to create resource cache")
    }
}
