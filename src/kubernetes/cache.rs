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
            .map(|c| {
                if c.is_alphanumeric() || c == '-' {
                    c
                } else {
                    '_'
                }
            })
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
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
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
    pub fn save_group(
        &self,
        group: &str,
        version: &str,
        resources: &[CachedResourceInfo],
    ) -> Result<()> {
        self.ensure_dirs()?;

        let cached = CachedGroup::new(group.to_string(), version.to_string(), resources.to_vec());
        let content =
            serde_json::to_string_pretty(&cached).context("Failed to serialize group cache")?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Create a test cache with a temporary directory
    fn test_cache() -> (ResourceCache, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let cache = ResourceCache {
            base_dir: temp_dir.path().to_path_buf(),
            ttl: DEFAULT_CACHE_TTL,
        };
        (cache, temp_dir)
    }

    /// Create a sample CachedResourceInfo for testing
    fn sample_cached_resource(name: &str, group: &str, version: &str) -> CachedResourceInfo {
        CachedResourceInfo {
            group: group.to_string(),
            version: version.to_string(),
            api_version: if group.is_empty() {
                version.to_string()
            } else {
                format!("{}/{}", group, version)
            },
            kind: name.to_string(),
            plural: format!("{}s", name.to_lowercase()),
            scope: "Namespaced".to_string(),
            table_name: format!("{}s", name.to_lowercase()),
            aliases: vec![name.to_lowercase()],
            is_core: false,
        }
    }

    #[test]
    fn test_group_cache_key_core() {
        // Empty group should become "core"
        assert_eq!(group_cache_key("", "v1"), "core_v1");
    }

    #[test]
    fn test_group_cache_key_simple() {
        assert_eq!(group_cache_key("apps", "v1"), "apps_v1");
    }

    #[test]
    fn test_group_cache_key_with_dots() {
        // Dots should be replaced with underscores
        assert_eq!(
            group_cache_key("cert-manager.io", "v1"),
            "cert-manager_io_v1"
        );
    }

    #[test]
    fn test_group_cache_key_complex() {
        assert_eq!(
            group_cache_key("networking.k8s.io", "v1"),
            "networking_k8s_io_v1"
        );
    }

    #[test]
    fn test_cached_resource_info_roundtrip() {
        // Create a ResourceInfo
        let resource_info = ResourceInfo {
            api_resource: ApiResource {
                group: "cert-manager.io".to_string(),
                version: "v1".to_string(),
                api_version: "cert-manager.io/v1".to_string(),
                kind: "Certificate".to_string(),
                plural: "certificates".to_string(),
            },
            capabilities: ApiCapabilities {
                scope: Scope::Namespaced,
                subresources: vec![],
                operations: vec![],
            },
            table_name: "certificates".to_string(),
            aliases: vec!["certificate".to_string(), "cert".to_string()],
            is_core: false,
            group: "cert-manager.io".to_string(),
            version: "v1".to_string(),
        };

        // Convert to cached format
        let cached = CachedResourceInfo::from(&resource_info);

        // Verify fields
        assert_eq!(cached.group, "cert-manager.io");
        assert_eq!(cached.version, "v1");
        assert_eq!(cached.kind, "Certificate");
        assert_eq!(cached.scope, "Namespaced");
        assert!(!cached.is_core);

        // Convert back
        let restored: ResourceInfo = cached.into();

        // Verify roundtrip
        assert_eq!(restored.api_resource.group, "cert-manager.io");
        assert_eq!(restored.api_resource.kind, "Certificate");
        assert_eq!(restored.table_name, "certificates");
        assert!(!restored.is_core);
    }

    #[test]
    fn test_cached_resource_info_cluster_scope() {
        let resource_info = ResourceInfo {
            api_resource: ApiResource {
                group: "".to_string(),
                version: "v1".to_string(),
                api_version: "v1".to_string(),
                kind: "Node".to_string(),
                plural: "nodes".to_string(),
            },
            capabilities: ApiCapabilities {
                scope: Scope::Cluster,
                subresources: vec![],
                operations: vec![],
            },
            table_name: "nodes".to_string(),
            aliases: vec!["node".to_string()],
            is_core: true,
            group: "".to_string(),
            version: "v1".to_string(),
        };

        let cached = CachedResourceInfo::from(&resource_info);
        assert_eq!(cached.scope, "Cluster");
        assert!(cached.is_core);

        let restored: ResourceInfo = cached.into();
        assert_eq!(restored.capabilities.scope, Scope::Cluster);
    }

    #[test]
    fn test_cluster_groups_freshness() {
        let groups = ClusterGroups::new(vec![
            ("apps".to_string(), "v1".to_string()),
            ("batch".to_string(), "v1".to_string()),
        ]);

        // Should be fresh with default TTL
        assert!(groups.is_fresh(Duration::from_secs(60)));

        // Should be fresh with 1 second TTL (just created)
        assert!(groups.is_fresh(Duration::from_secs(1)));

        // Should not be fresh with 0 TTL
        assert!(!groups.is_fresh(Duration::from_secs(0)));
    }

    #[test]
    fn test_cached_group_freshness() {
        let group = CachedGroup::new(
            "cert-manager.io".to_string(),
            "v1".to_string(),
            vec![sample_cached_resource(
                "Certificate",
                "cert-manager.io",
                "v1",
            )],
        );

        // Should be fresh with default TTL
        assert!(group.is_fresh(Duration::from_secs(60)));

        // Should not be fresh with 0 TTL
        assert!(!group.is_fresh(Duration::from_secs(0)));
    }

    #[test]
    fn test_save_and_load_group() {
        let (cache, _temp_dir) = test_cache();

        let resources = vec![
            sample_cached_resource("Certificate", "cert-manager.io", "v1"),
            sample_cached_resource("Issuer", "cert-manager.io", "v1"),
        ];

        // Save the group
        cache
            .save_group("cert-manager.io", "v1", &resources)
            .expect("Failed to save group");

        // Load it back
        let loaded = cache
            .load_group("cert-manager.io", "v1")
            .expect("Failed to load group");

        assert_eq!(loaded.group, "cert-manager.io");
        assert_eq!(loaded.version, "v1");
        assert_eq!(loaded.resources.len(), 2);
        assert_eq!(loaded.resources[0].kind, "Certificate");
        assert_eq!(loaded.resources[1].kind, "Issuer");
    }

    #[test]
    fn test_save_and_load_cluster_groups() {
        let (cache, _temp_dir) = test_cache();

        let groups = vec![
            ("apps".to_string(), "v1".to_string()),
            ("cert-manager.io".to_string(), "v1".to_string()),
            ("".to_string(), "v1".to_string()), // core API
        ];

        // Save cluster groups
        cache
            .save_cluster_groups("test-cluster", &groups)
            .expect("Failed to save cluster groups");

        // Load them back
        let loaded = cache
            .load_cluster_groups("test-cluster")
            .expect("Failed to load cluster groups");

        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains(&("apps".to_string(), "v1".to_string())));
        assert!(loaded.contains(&("cert-manager.io".to_string(), "v1".to_string())));
    }

    #[test]
    fn test_check_groups_all_cached() {
        let (cache, _temp_dir) = test_cache();

        // Save some groups
        cache
            .save_group(
                "apps",
                "v1",
                &[sample_cached_resource("Deployment", "apps", "v1")],
            )
            .unwrap();
        cache
            .save_group(
                "batch",
                "v1",
                &[sample_cached_resource("Job", "batch", "v1")],
            )
            .unwrap();

        // Check groups - all should be cached
        let api_groups = vec![
            ("apps".to_string(), "v1".to_string()),
            ("batch".to_string(), "v1".to_string()),
        ];

        let (cached, missing) = cache.check_groups(&api_groups);

        assert_eq!(cached.len(), 2);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_check_groups_some_missing() {
        let (cache, _temp_dir) = test_cache();

        // Save only one group
        cache
            .save_group(
                "apps",
                "v1",
                &[sample_cached_resource("Deployment", "apps", "v1")],
            )
            .unwrap();

        // Check groups - one cached, one missing
        let api_groups = vec![
            ("apps".to_string(), "v1".to_string()),
            ("batch".to_string(), "v1".to_string()),
        ];

        let (cached, missing) = cache.check_groups(&api_groups);

        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].group, "apps");
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0], ("batch".to_string(), "v1".to_string()));
    }

    #[test]
    fn test_check_groups_all_missing() {
        let (cache, _temp_dir) = test_cache();

        // Don't save anything
        let api_groups = vec![
            ("apps".to_string(), "v1".to_string()),
            ("batch".to_string(), "v1".to_string()),
        ];

        let (cached, missing) = cache.check_groups(&api_groups);

        assert!(cached.is_empty());
        assert_eq!(missing.len(), 2);
    }

    #[test]
    fn test_clear_cluster() {
        let (cache, _temp_dir) = test_cache();

        // Save cluster groups
        cache
            .save_cluster_groups("test-cluster", &[("apps".to_string(), "v1".to_string())])
            .unwrap();

        // Verify it exists
        assert!(cache.load_cluster_groups("test-cluster").is_some());

        // Clear it
        cache.clear("test-cluster").unwrap();

        // Verify it's gone
        assert!(cache.load_cluster_groups("test-cluster").is_none());
    }

    #[test]
    fn test_clear_all() {
        let (cache, _temp_dir) = test_cache();

        // Save some data
        cache
            .save_cluster_groups("cluster1", &[("apps".to_string(), "v1".to_string())])
            .unwrap();
        cache
            .save_group(
                "apps",
                "v1",
                &[sample_cached_resource("Deployment", "apps", "v1")],
            )
            .unwrap();

        // Clear all
        cache.clear_all().unwrap();

        // Verify everything is gone
        assert!(cache.load_cluster_groups("cluster1").is_none());
        assert!(cache.load_group("apps", "v1").is_none());
    }

    #[test]
    fn test_cluster_name_sanitization() {
        let (cache, _temp_dir) = test_cache();

        // Cluster name with special characters
        let cluster_name = "arn:aws:eks:us-west-2:123456789:cluster/my-cluster";

        cache
            .save_cluster_groups(cluster_name, &[("apps".to_string(), "v1".to_string())])
            .unwrap();

        // Should be able to load it back
        let loaded = cache.load_cluster_groups(cluster_name);
        assert!(loaded.is_some());
    }
}
