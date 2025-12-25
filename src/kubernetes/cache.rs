// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Local cache for discovered Kubernetes resources (CRDs)
//!
//! Caches CRD discovery results to enable fast startup on subsequent runs.
//! Uses fingerprint-based caching so clusters with identical CRDs share cache entries.
//! Core K8s resources are always loaded from k8s-openapi (instant).

use anyhow::{Context, Result};
use kube::discovery::{ApiCapabilities, ApiResource, Scope};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use super::discovery::ResourceInfo;

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

/// Cluster API groups list (which groups this cluster has)
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterGroups {
    /// List of (group, version) pairs
    pub groups: Vec<(String, String)>,
    /// When this was cached
    pub created_at: u64,
}

impl ClusterGroups {
    pub fn new(groups: Vec<(String, String)>) -> Self {
        Self {
            groups,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    #[allow(dead_code)]
    pub fn is_fresh(&self, ttl: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.saturating_sub(self.created_at);
        age < ttl.as_secs()
    }
}

/// Cached resources for a single API group
#[derive(Debug, Serialize, Deserialize)]
pub struct CachedGroup {
    /// API group name (empty string for core)
    pub group: String,
    /// API version
    pub version: String,
    /// When this was cached
    pub created_at: u64,
    /// Resources in this group
    pub resources: Vec<CachedResourceInfo>,
}

impl CachedGroup {
    pub fn new(group: String, version: String, resources: Vec<CachedResourceInfo>) -> Self {
        Self {
            group,
            version,
            created_at: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            resources,
        }
    }

    pub fn is_fresh(&self, ttl: Duration) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let age = now.saturating_sub(self.created_at);
        age < ttl.as_secs()
    }
}

/// Compute a cache key for an API group
pub fn group_cache_key(group: &str, version: &str) -> String {
    // Sanitize group name for filesystem (replace dots with underscores)
    let safe_group: String = if group.is_empty() {
        "core".to_string()
    } else {
        group
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' { c } else { '_' })
            .collect()
    };
    format!("{}_{}", safe_group, version)
}

/// Resource cache manager with per-API-group caching
///
/// Cache structure:
/// ```
/// ~/.cache/k8sql/
///   groups/
///     <group>_<version>.json   # Resources for this API group (shared across clusters)
///   clusters/
///     <cluster>.json           # List of (group, version) pairs this cluster has
/// ```
///
/// This enables efficient incremental updates:
/// - If a cluster adds one new CRD, only that API group needs to be discovered
/// - Common CRDs (cert-manager, istio, etc.) are shared across clusters
pub struct ResourceCache {
    base_dir: PathBuf,
    ttl: Duration,
}

impl ResourceCache {
    /// Create a new cache manager
    pub fn new() -> Result<Self> {
        let base_dir = Self::default_cache_dir()?;
        Ok(Self {
            base_dir,
            ttl: DEFAULT_CACHE_TTL,
        })
    }

    /// Get the default cache directory
    fn default_cache_dir() -> Result<PathBuf> {
        let cache_base = dirs::cache_dir()
            .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
            .context("Could not determine cache directory")?;
        Ok(cache_base.join("k8sql"))
    }

    /// Get the clusters directory
    fn clusters_dir(&self) -> PathBuf {
        self.base_dir.join("clusters")
    }

    /// Get the groups directory
    fn groups_dir(&self) -> PathBuf {
        self.base_dir.join("groups")
    }

    /// Get the cluster mapping file path
    fn cluster_path(&self, cluster: &str) -> PathBuf {
        let safe_name: String = cluster
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.clusters_dir().join(format!("{}.json", safe_name))
    }

    /// Get the group cache file path
    fn group_path(&self, group: &str, version: &str) -> PathBuf {
        let key = group_cache_key(group, version);
        self.groups_dir().join(format!("{}.json", key))
    }

    /// Ensure cache directories exist
    fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(self.clusters_dir())
            .context("Failed to create clusters cache directory")?;
        std::fs::create_dir_all(self.groups_dir())
            .context("Failed to create groups cache directory")?;
        Ok(())
    }

    /// Load the list of API groups for a cluster (if cached and fresh)
    #[allow(dead_code)]
    pub fn load_cluster_groups(&self, cluster: &str) -> Option<Vec<(String, String)>> {
        let path = self.cluster_path(cluster);
        if !path.exists() {
            return None;
        }

        let content = std::fs::read_to_string(&path).ok()?;
        let cluster_groups: ClusterGroups = serde_json::from_str(&content).ok()?;

        if cluster_groups.is_fresh(self.ttl) {
            Some(cluster_groups.groups)
        } else {
            let _ = std::fs::remove_file(&path);
            None
        }
    }

    /// Load a single API group's resources (if cached and fresh)
    pub fn load_group(&self, group: &str, version: &str) -> Option<CachedGroup> {
        let path = self.group_path(group, version);
        if !path.exists() {
            return None;
        }

        let content = std::fs::read_to_string(&path).ok()?;
        let cached: CachedGroup = serde_json::from_str(&content).ok()?;

        if cached.is_fresh(self.ttl) {
            Some(cached)
        } else {
            let _ = std::fs::remove_file(&path);
            None
        }
    }

    /// Check which API groups are missing from cache
    /// Returns (cached_groups, missing_groups)
    pub fn check_groups(
        &self,
        api_groups: &[(String, String)],
    ) -> (Vec<CachedGroup>, Vec<(String, String)>) {
        let mut cached = Vec::new();
        let mut missing = Vec::new();

        for (group, version) in api_groups {
            if let Some(cached_group) = self.load_group(group, version) {
                cached.push(cached_group);
            } else {
                missing.push((group.clone(), version.clone()));
            }
        }

        (cached, missing)
    }

    /// Save the list of API groups for a cluster
    pub fn save_cluster_groups(&self, cluster: &str, groups: &[(String, String)]) -> Result<()> {
        self.ensure_dirs()?;

        let cluster_groups = ClusterGroups::new(groups.to_vec());
        let content = serde_json::to_string_pretty(&cluster_groups)
            .context("Failed to serialize cluster groups")?;

        let path = self.cluster_path(cluster);
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write cluster groups to {:?}", path))?;

        Ok(())
    }

    /// Save a single API group's resources
    pub fn save_group(&self, group: &str, version: &str, resources: &[CachedResourceInfo]) -> Result<()> {
        self.ensure_dirs()?;

        let cached = CachedGroup::new(group.to_string(), version.to_string(), resources.to_vec());
        let content = serde_json::to_string_pretty(&cached)
            .context("Failed to serialize group cache")?;

        let path = self.group_path(group, version);
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write group cache to {:?}", path))?;

        Ok(())
    }

    /// Clear cache for a specific cluster
    #[allow(dead_code)]
    pub fn clear(&self, cluster: &str) -> Result<()> {
        let path = self.cluster_path(cluster);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Clear all cached data
    #[allow(dead_code)]
    pub fn clear_all(&self) -> Result<()> {
        if self.base_dir.exists() {
            std::fs::remove_dir_all(&self.base_dir)?;
        }
        Ok(())
    }
}

impl Default for ResourceCache {
    fn default() -> Self {
        Self::new().expect("Failed to create resource cache")
    }
}
