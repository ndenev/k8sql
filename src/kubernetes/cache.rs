// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Local cache for discovered Kubernetes resources (CRDs)
//!
//! Caches CRD discovery results to enable fast startup on subsequent runs.
//! Uses fingerprint-based caching so clusters with identical CRDs share cache entries.
//! Core K8s resources are always loaded from k8s-openapi (instant).
//!
//! Cache location: ~/.k8sql/cache/

use anyhow::{Context, Result};
use kube::discovery::{ApiCapabilities, ApiResource, Scope};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use super::discovery::{ColumnDataType, ColumnDef, ResourceInfo};
use crate::config;

/// Get current UNIX timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Atomically write content to a file using tempfile + rename
///
/// Creates a temporary file in the same directory, writes content, then
/// atomically renames it to the final path. This ensures:
/// - Crash safety: Incomplete writes don't corrupt the target file
/// - Multi-process safety: Other processes see either old or new content, never partial
fn atomic_write(path: &std::path::Path, content: &[u8]) -> Result<()> {
    use tempfile::NamedTempFile;

    // Create temp file in same directory as target
    let temp_file =
        NamedTempFile::new_in(path.parent().unwrap_or_else(|| std::path::Path::new(".")))
            .context("Failed to create temp file")?;

    // Write content to temp file
    std::fs::write(temp_file.path(), content)
        .with_context(|| format!("Failed to write temp file {:?}", temp_file.path()))?;

    // Atomically rename to final path (atomic on Unix-like systems)
    temp_file
        .persist(path)
        .with_context(|| format!("Failed to persist file to {:?}", path))?;

    Ok(())
}

/// TTL for cluster groups list (1 hour)
/// This determines how often we check which CRDs a cluster has
const CLUSTER_GROUPS_TTL: Duration = Duration::from_secs(60 * 60);

/// TTL for CRD schemas (indefinite)
/// CRD schemas rarely change, so we cache them forever and only
/// refresh on error or explicit --refresh-crds flag
const SCHEMA_CACHE_TTL: Option<Duration> = None;

/// Serializable version of ColumnDef for caching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedColumnDef {
    pub name: String,
    pub data_type: String, // "Text", "Timestamp", or "Integer"
    pub json_path: Option<String>,
    #[serde(default)]
    pub is_json_object: bool,
}

impl From<&ColumnDef> for CachedColumnDef {
    fn from(col: &ColumnDef) -> Self {
        Self {
            name: col.name.clone(),
            data_type: match col.data_type {
                ColumnDataType::Text => "Text".to_string(),
                ColumnDataType::Timestamp => "Timestamp".to_string(),
                ColumnDataType::Integer => "Integer".to_string(),
            },
            json_path: col.json_path.clone(),
            is_json_object: col.is_json_object,
        }
    }
}

impl From<CachedColumnDef> for ColumnDef {
    fn from(cached: CachedColumnDef) -> Self {
        Self {
            name: cached.name.clone(),
            data_type: match cached.data_type.as_str() {
                "Timestamp" => ColumnDataType::Timestamp,
                "Integer" => ColumnDataType::Integer,
                _ => ColumnDataType::Text,
            },
            json_path: cached.json_path,
            is_json_object: cached.is_json_object,
        }
    }
}

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
    // CRD schema fields (extracted from OpenAPI definition)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_fields: Option<Vec<CachedColumnDef>>,
    // Cache metadata
    pub created_at: u64,
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
            custom_fields: info
                .custom_fields
                .as_ref()
                .map(|fields| fields.iter().map(CachedColumnDef::from).collect()),
            created_at: current_timestamp(),
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
            custom_fields: cached
                .custom_fields
                .map(|fields| fields.into_iter().map(ColumnDef::from).collect()),
        }
    }
}

/// Cluster CRD list (which CRDs this cluster has)
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterCRDs {
    /// List of (group, version, kind) tuples
    pub crds: Vec<(String, String, String)>,
    /// When this was cached
    pub created_at: u64,
}

/// Check if a cached entry is still fresh based on creation time and TTL
fn is_cache_fresh(created_at: u64, ttl: Duration) -> bool {
    let now = current_timestamp();
    now.saturating_sub(created_at) < ttl.as_secs()
}

impl ClusterCRDs {
    pub fn new(crds: Vec<(String, String, String)>) -> Self {
        Self {
            crds,
            created_at: current_timestamp(),
        }
    }

    pub fn is_fresh(&self, ttl: Duration) -> bool {
        is_cache_fresh(self.created_at, ttl)
    }
}

/// Sanitize a string for use as a filename
/// Replaces non-alphanumeric characters (except dash, and optionally underscore) with underscore
fn sanitize_filename(name: &str, allow_underscore: bool) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || (allow_underscore && c == '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Compute a cache key for a CRD (group_version format for directory, kind for filename)
pub fn crd_cache_key(group: &str, version: &str, kind: &str) -> (String, String) {
    let safe_group = if group.is_empty() {
        "core".to_string()
    } else {
        sanitize_filename(group, false)
    };
    let dir_name = format!("{}_{}", safe_group, version);
    let file_name = format!("{}.json", sanitize_filename(kind, false));
    (dir_name, file_name)
}

/// Resource cache manager with per-CRD caching
///
/// Cache structure:
/// ```
/// ~/.cache/k8sql/
///   crds/
///     <group>_<version>/
///       <kind>.json            # Individual CRD cached independently
///   clusters/
///     <cluster>.json           # List of (group, version, kind) tuples this cluster has
/// ```
///
/// Example:
/// ```
/// ~/.cache/k8sql/
///   crds/
///     core_v1/
///       Pod.json
///       Service.json
///     cert-manager_io_v1/
///       Certificate.json
///       Issuer.json
///   clusters/
///     prod-cluster.json        # [("apps", "v1", "Deployment"), ...]
/// ```
///
/// This enables correct multi-cluster caching:
/// - Different clusters can have different CRDs in the same API group
/// - Each CRD cached independently (no false cache hits)
/// - Common CRDs are still shared across clusters (same file)
///
/// Caching strategy:
/// - Cluster CRD list: 1 hour TTL (detect new/removed CRDs)
/// - CRD schemas: Indefinite (schemas rarely change, refresh via --refresh-crds)
///
/// Thread safety:
/// - write_lock protects concurrent writes from multiple threads
/// - Atomic file writes (tempfile + rename) protect against crashes and multiple processes
pub struct ResourceCache {
    base_dir: PathBuf,
    /// Protects cache writes from concurrent threads
    write_lock: std::sync::Mutex<()>,
}

impl ResourceCache {
    /// Create a new cache manager
    pub fn new() -> Result<Self> {
        let base_dir = Self::default_cache_dir()?;
        let cache = Self {
            base_dir,
            write_lock: std::sync::Mutex::new(()),
        };

        // Warn about old cache directory from previous versions
        cache.warn_old_cache_dir();

        Ok(cache)
    }

    /// Get the default cache directory (~/.k8sql/cache/)
    fn default_cache_dir() -> Result<PathBuf> {
        Ok(config::base_dir()?.join("cache"))
    }

    /// Get the clusters directory
    fn clusters_dir(&self) -> PathBuf {
        self.base_dir.join("clusters")
    }

    /// Get the CRDs directory
    fn crds_dir(&self) -> PathBuf {
        self.base_dir.join("crds")
    }

    /// Get the cluster mapping file path
    fn cluster_path(&self, cluster: &str) -> PathBuf {
        let safe_name = sanitize_filename(cluster, true);
        self.clusters_dir().join(format!("{}.json", safe_name))
    }

    /// Get the CRD cache file path (two-level structure: crds/{group}_{version}/{kind}.json)
    fn crd_path(&self, group: &str, version: &str, kind: &str) -> PathBuf {
        let (dir_name, file_name) = crd_cache_key(group, version, kind);
        self.crds_dir().join(dir_name).join(file_name)
    }

    /// Ensure cache directories exist
    fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(self.clusters_dir())
            .context("Failed to create clusters cache directory")?;
        std::fs::create_dir_all(self.crds_dir())
            .context("Failed to create CRDs cache directory")?;
        Ok(())
    }

    /// Warn about old cache directory from previous versions
    /// The cache structure changed from ~/.k8sql/cache/groups/ to ~/.k8sql/cache/crds/
    fn warn_old_cache_dir(&self) {
        let old_groups_dir = self.base_dir.join("groups");
        if old_groups_dir.exists() {
            eprintln!();
            eprintln!("⚠️  Cache Migration Notice:");
            eprintln!("   Found old cache directory from a previous version.");
            eprintln!("   You can safely delete it to free up disk space:");
            eprintln!("   rm -rf {}", old_groups_dir.display());
            eprintln!();
        }
    }

    /// Load the list of CRDs for a cluster (if cached and fresh)
    /// Uses CLUSTER_GROUPS_TTL (1 hour) to detect new/removed CRDs
    pub fn load_cluster_groups(&self, cluster: &str) -> Option<Vec<(String, String, String)>> {
        let path = self.cluster_path(cluster);
        if !path.exists() {
            return None;
        }

        let content = std::fs::read_to_string(&path).ok()?;
        let cluster_crds: ClusterCRDs = serde_json::from_str(&content).ok()?;

        if cluster_crds.is_fresh(CLUSTER_GROUPS_TTL) {
            Some(cluster_crds.crds)
        } else {
            let _ = std::fs::remove_file(&path);
            None
        }
    }

    /// Load a single CRD's resource (cached indefinitely)
    /// CRD schemas rarely change, so we cache them forever.
    /// Use --refresh-crds or clear the cache to force a refresh.
    pub fn load_crd(&self, group: &str, version: &str, kind: &str) -> Option<CachedResourceInfo> {
        let path = self.crd_path(group, version, kind);
        if !path.exists() {
            return None;
        }

        let content = std::fs::read_to_string(&path).ok()?;
        let cached: CachedResourceInfo = serde_json::from_str(&content)
            .map_err(|e| {
                eprintln!("⚠️  Failed to parse cache file: {}", path.display());
                eprintln!("   Error: {}", e);
                eprintln!("   The cache may be outdated. To fix, run: rm -rf ~/.k8sql/cache");
                e
            })
            .ok()?;

        // CRD schemas are cached indefinitely (SCHEMA_CACHE_TTL = None)
        // Only refresh on error or explicit --refresh-crds flag
        match SCHEMA_CACHE_TTL {
            Some(ttl) => {
                if is_cache_fresh(cached.created_at, ttl) {
                    Some(cached)
                } else {
                    let _ = std::fs::remove_file(&path);
                    None
                }
            }
            None => Some(cached), // Indefinite caching
        }
    }

    /// Check which CRDs are missing from cache
    /// Returns (cached_crds, missing_crds)
    pub fn check_crds(
        &self,
        crds: &[(String, String, String)],
    ) -> (Vec<CachedResourceInfo>, Vec<(String, String, String)>) {
        let mut cached = Vec::new();
        let mut missing = Vec::new();

        for (group, version, kind) in crds {
            if let Some(cached_crd) = self.load_crd(group, version, kind) {
                cached.push(cached_crd);
            } else {
                missing.push((group.clone(), version.clone(), kind.clone()));
            }
        }

        (cached, missing)
    }

    /// Save the list of CRDs for a cluster with thread-safe atomic writes
    pub fn save_cluster_groups(
        &self,
        cluster: &str,
        crds: &[(String, String, String)],
    ) -> Result<()> {
        self.ensure_dirs()?;

        // Serialize first (outside the lock)
        let cluster_crds = ClusterCRDs::new(crds.to_vec());
        let content = serde_json::to_string_pretty(&cluster_crds)
            .context("Failed to serialize cluster CRDs")?;

        // Acquire write lock for thread safety
        let _lock = self.write_lock.lock().unwrap();

        let path = self.cluster_path(cluster);

        // Atomic write with tempfile + rename for crash and multi-process safety
        atomic_write(&path, content.as_bytes()).context("Failed to write cluster CRDs")?;

        Ok(())
    }

    /// Save a single CRD's resource with thread-safe atomic writes
    ///
    /// Uses both:
    /// - Mutex for thread safety (parallel cluster discovery)
    /// - Atomic writes (tempfile + rename) for crash safety and multi-process safety
    pub fn save_crd(
        &self,
        group: &str,
        version: &str,
        kind: &str,
        resource: &CachedResourceInfo,
    ) -> Result<()> {
        // Serialize first (outside the lock)
        let content =
            serde_json::to_string_pretty(resource).context("Failed to serialize CRD cache")?;

        // Acquire write lock for thread safety
        let _lock = self.write_lock.lock().unwrap();

        let path = self.crd_path(group, version, kind);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create CRD cache directory {:?}", parent))?;
        }

        // Atomic write with tempfile + rename for crash and multi-process safety
        atomic_write(&path, content.as_bytes()).context("Failed to write CRD cache")?;

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

    /// Clear all cached CRD schemas (crds directory)
    /// Called when --refresh-crds flag is used
    pub fn clear_crds(&self) -> Result<()> {
        let crds_dir = self.crds_dir();
        if crds_dir.exists() {
            std::fs::remove_dir_all(&crds_dir)?;
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
            write_lock: std::sync::Mutex::new(()),
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
            custom_fields: None,
            created_at: current_timestamp(),
        }
    }

    #[test]
    fn test_crd_cache_key_core() {
        // Empty group should become "core"
        let (dir, file) = crd_cache_key("", "v1", "Pod");
        assert_eq!(dir, "core_v1");
        assert_eq!(file, "Pod.json");
    }

    #[test]
    fn test_crd_cache_key_simple() {
        let (dir, file) = crd_cache_key("apps", "v1", "Deployment");
        assert_eq!(dir, "apps_v1");
        assert_eq!(file, "Deployment.json");
    }

    #[test]
    fn test_crd_cache_key_with_dots() {
        // Dots should be replaced with underscores
        let (dir, file) = crd_cache_key("cert-manager.io", "v1", "Certificate");
        assert_eq!(dir, "cert-manager_io_v1");
        assert_eq!(file, "Certificate.json");
    }

    #[test]
    fn test_crd_cache_key_complex() {
        let (dir, file) = crd_cache_key("networking.k8s.io", "v1", "Ingress");
        assert_eq!(dir, "networking_k8s_io_v1");
        assert_eq!(file, "Ingress.json");
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
            custom_fields: None,
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
            custom_fields: None,
        };

        let cached = CachedResourceInfo::from(&resource_info);
        assert_eq!(cached.scope, "Cluster");
        assert!(cached.is_core);

        let restored: ResourceInfo = cached.into();
        assert_eq!(restored.capabilities.scope, Scope::Cluster);
    }

    #[test]
    fn test_cluster_crds_freshness() {
        let crds = ClusterCRDs::new(vec![
            (
                "apps".to_string(),
                "v1".to_string(),
                "Deployment".to_string(),
            ),
            ("batch".to_string(), "v1".to_string(), "Job".to_string()),
        ]);

        // Should be fresh with default TTL
        assert!(crds.is_fresh(Duration::from_secs(60)));

        // Should be fresh with 1 second TTL (just created)
        assert!(crds.is_fresh(Duration::from_secs(1)));

        // Should not be fresh with 0 TTL
        assert!(!crds.is_fresh(Duration::from_secs(0)));
    }

    #[test]
    fn test_cached_resource_freshness() {
        let resource = sample_cached_resource("Certificate", "cert-manager.io", "v1");

        // Should be fresh with default TTL
        assert!(is_cache_fresh(resource.created_at, Duration::from_secs(60)));

        // Should not be fresh with 0 TTL
        assert!(!is_cache_fresh(resource.created_at, Duration::from_secs(0)));
    }

    #[test]
    fn test_save_and_load_crd() {
        let (cache, _temp_dir) = test_cache();

        let certificate = sample_cached_resource("Certificate", "cert-manager.io", "v1");
        let issuer = sample_cached_resource("Issuer", "cert-manager.io", "v1");

        // Save individual CRDs
        cache
            .save_crd("cert-manager.io", "v1", "Certificate", &certificate)
            .expect("Failed to save Certificate CRD");
        cache
            .save_crd("cert-manager.io", "v1", "Issuer", &issuer)
            .expect("Failed to save Issuer CRD");

        // Load them back
        let loaded_cert = cache
            .load_crd("cert-manager.io", "v1", "Certificate")
            .expect("Failed to load Certificate CRD");
        let loaded_issuer = cache
            .load_crd("cert-manager.io", "v1", "Issuer")
            .expect("Failed to load Issuer CRD");

        assert_eq!(loaded_cert.group, "cert-manager.io");
        assert_eq!(loaded_cert.version, "v1");
        assert_eq!(loaded_cert.kind, "Certificate");

        assert_eq!(loaded_issuer.group, "cert-manager.io");
        assert_eq!(loaded_issuer.version, "v1");
        assert_eq!(loaded_issuer.kind, "Issuer");
    }

    #[test]
    fn test_save_and_load_cluster_crds() {
        let (cache, _temp_dir) = test_cache();

        let crds = vec![
            (
                "apps".to_string(),
                "v1".to_string(),
                "Deployment".to_string(),
            ),
            (
                "cert-manager.io".to_string(),
                "v1".to_string(),
                "Certificate".to_string(),
            ),
            ("".to_string(), "v1".to_string(), "Pod".to_string()), // core API
        ];

        // Save cluster CRDs
        cache
            .save_cluster_groups("test-cluster", &crds)
            .expect("Failed to save cluster CRDs");

        // Load them back
        let loaded = cache
            .load_cluster_groups("test-cluster")
            .expect("Failed to load cluster CRDs");

        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains(&(
            "apps".to_string(),
            "v1".to_string(),
            "Deployment".to_string()
        )));
        assert!(loaded.contains(&(
            "cert-manager.io".to_string(),
            "v1".to_string(),
            "Certificate".to_string()
        )));
        assert!(loaded.contains(&("".to_string(), "v1".to_string(), "Pod".to_string())));
    }

    #[test]
    fn test_check_crds_all_cached() {
        let (cache, _temp_dir) = test_cache();

        // Save some CRDs
        cache
            .save_crd(
                "apps",
                "v1",
                "Deployment",
                &sample_cached_resource("Deployment", "apps", "v1"),
            )
            .unwrap();
        cache
            .save_crd(
                "batch",
                "v1",
                "Job",
                &sample_cached_resource("Job", "batch", "v1"),
            )
            .unwrap();

        // Check CRDs - all should be cached
        let crds = vec![
            (
                "apps".to_string(),
                "v1".to_string(),
                "Deployment".to_string(),
            ),
            ("batch".to_string(), "v1".to_string(), "Job".to_string()),
        ];

        let (cached, missing) = cache.check_crds(&crds);

        assert_eq!(cached.len(), 2);
        assert!(missing.is_empty());
    }

    #[test]
    fn test_check_crds_some_missing() {
        let (cache, _temp_dir) = test_cache();

        // Save only one CRD
        cache
            .save_crd(
                "apps",
                "v1",
                "Deployment",
                &sample_cached_resource("Deployment", "apps", "v1"),
            )
            .unwrap();

        // Check CRDs - one cached, one missing
        let crds = vec![
            (
                "apps".to_string(),
                "v1".to_string(),
                "Deployment".to_string(),
            ),
            ("batch".to_string(), "v1".to_string(), "Job".to_string()),
        ];

        let (cached, missing) = cache.check_crds(&crds);

        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].group, "apps");
        assert_eq!(cached[0].kind, "Deployment");
        assert_eq!(missing.len(), 1);
        assert_eq!(
            missing[0],
            ("batch".to_string(), "v1".to_string(), "Job".to_string())
        );
    }

    #[test]
    fn test_check_crds_all_missing() {
        let (cache, _temp_dir) = test_cache();

        // Don't save anything
        let crds = vec![
            (
                "apps".to_string(),
                "v1".to_string(),
                "Deployment".to_string(),
            ),
            ("batch".to_string(), "v1".to_string(), "Job".to_string()),
        ];

        let (cached, missing) = cache.check_crds(&crds);

        assert!(cached.is_empty());
        assert_eq!(missing.len(), 2);
    }

    #[test]
    fn test_clear_cluster() {
        let (cache, _temp_dir) = test_cache();

        // Save cluster CRDs
        cache
            .save_cluster_groups(
                "test-cluster",
                &[(
                    "apps".to_string(),
                    "v1".to_string(),
                    "Deployment".to_string(),
                )],
            )
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
            .save_cluster_groups(
                "cluster1",
                &[(
                    "apps".to_string(),
                    "v1".to_string(),
                    "Deployment".to_string(),
                )],
            )
            .unwrap();
        cache
            .save_crd(
                "apps",
                "v1",
                "Deployment",
                &sample_cached_resource("Deployment", "apps", "v1"),
            )
            .unwrap();

        // Clear all
        cache.clear_all().unwrap();

        // Verify everything is gone
        assert!(cache.load_cluster_groups("cluster1").is_none());
        assert!(cache.load_crd("apps", "v1", "Deployment").is_none());
    }

    #[test]
    fn test_cluster_name_sanitization() {
        let (cache, _temp_dir) = test_cache();

        // Cluster name with special characters
        let cluster_name = "arn:aws:eks:us-west-2:123456789:cluster/my-cluster";

        cache
            .save_cluster_groups(
                cluster_name,
                &[(
                    "apps".to_string(),
                    "v1".to_string(),
                    "Deployment".to_string(),
                )],
            )
            .unwrap();

        // Should be able to load it back
        let loaded = cache.load_cluster_groups(cluster_name);
        assert!(loaded.is_some());
    }
}
