// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Resource discovery for Kubernetes clusters.
//!
//! Discovers all available resources (including CRDs) at runtime using
//! the Kubernetes discovery API.

use anyhow::Result;
use kube::Client;
use kube::discovery::{ApiCapabilities, ApiResource, Discovery, Scope};
use std::collections::HashMap;

/// Column definition for schema
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnDef {
    pub name: String,
    /// SQL type (text, jsonb, timestamp, integer) - reserved for future typed schema
    pub data_type: String,
    pub json_path: Option<String>,
    /// Human-readable description - reserved for enhanced DESCRIBE output
    pub description: String,
}

/// Information about a discovered Kubernetes resource
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ResourceInfo {
    /// The API resource definition
    pub api_resource: ApiResource,
    /// API capabilities (verbs, scope, etc.)
    pub capabilities: ApiCapabilities,
    /// SQL table name (lowercase, e.g., "pods", "certificates")
    pub table_name: String,
    /// Table aliases (e.g., "pod" for "pods", "cert" for "certificates")
    pub aliases: Vec<String>,
    /// Whether this is a core resource (has static type) or dynamic (CRD)
    pub is_core: bool,
    /// API group (empty string for core v1)
    pub group: String,
    /// API version
    pub version: String,
}

impl ResourceInfo {
    /// Check if this resource is namespace-scoped
    pub fn is_namespaced(&self) -> bool {
        self.capabilities.scope == Scope::Namespaced
    }

    /// Get the full API group/version string
    #[allow(dead_code)]
    pub fn api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
}

/// Registry of all discovered resources for a cluster
#[derive(Debug, Clone)]
pub struct ResourceRegistry {
    /// Resources indexed by table name
    by_table_name: HashMap<String, ResourceInfo>,
    /// Alias to table name mapping
    alias_map: HashMap<String, String>,
}

impl ResourceRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            by_table_name: HashMap::new(),
            alias_map: HashMap::new(),
        }
    }

    /// Add a resource to the registry
    /// Core resources (from CORE_RESOURCES) take priority for the primary table name
    /// Non-core resources with conflicting names are registered under their Kind name
    pub fn add(&mut self, mut info: ResourceInfo) {
        let original_table_name = info.table_name.clone();

        // Check if this table already exists
        if let Some(existing) = self.by_table_name.get(&original_table_name) {
            if existing.is_core && !info.is_core {
                // Core exists, rename incoming non-core to its Kind name
                let kind_name = info.api_resource.kind.to_lowercase();
                if self.by_table_name.contains_key(&kind_name) {
                    return; // Kind name also conflicts, skip
                }
                info.table_name = kind_name;
                info.aliases = vec![];
            } else if !existing.is_core && info.is_core {
                // Non-core exists, core is arriving - move non-core to Kind name first
                let existing_owned = existing.clone();
                let kind_name = existing_owned.api_resource.kind.to_lowercase();

                if !self.by_table_name.contains_key(&kind_name) {
                    // Re-register existing non-core under its Kind name
                    let mut renamed = existing_owned;
                    renamed.table_name = kind_name.clone();
                    renamed.aliases = vec![];

                    self.alias_map.insert(kind_name.clone(), kind_name.clone());
                    self.by_table_name.insert(kind_name, renamed);
                }
                // Remove the old entry's alias mapping (will be replaced by core)
                // Core will be added normally below
            } else if !existing.is_core && !info.is_core {
                // Both non-core with same name - rename incoming to Kind name
                let kind_name = info.api_resource.kind.to_lowercase();
                if self.by_table_name.contains_key(&kind_name) {
                    return;
                }
                info.table_name = kind_name;
                info.aliases = vec![];
            }
            // If both are core (shouldn't happen), later one wins
        }

        // Add aliases
        for alias in &info.aliases {
            self.alias_map
                .insert(alias.clone(), info.table_name.clone());
        }
        // Add the table name itself as an alias
        self.alias_map
            .insert(info.table_name.clone(), info.table_name.clone());
        // Store the resource info
        self.by_table_name.insert(info.table_name.clone(), info);
    }

    /// Look up a resource by table name or alias
    pub fn get(&self, name: &str) -> Option<&ResourceInfo> {
        let table_name = self.alias_map.get(&name.to_lowercase())?;
        self.by_table_name.get(table_name)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<&ResourceInfo> {
        let mut resources: Vec<_> = self.by_table_name.values().collect();
        resources.sort_by(|a, b| a.table_name.cmp(&b.table_name));
        resources
    }

    /// Merge another registry into this one
    /// Resources from the other registry are added using the normal add() logic
    pub fn merge(&mut self, other: ResourceRegistry) {
        for info in other.by_table_name.into_values() {
            self.add(info);
        }
    }
}

/// Build a registry with just core resources using k8s-openapi types (no discovery, instant startup)
///
/// This uses compile-time type information from k8s-openapi, so it automatically
/// stays in sync with the Kubernetes API version we're building against.
pub fn build_core_registry() -> ResourceRegistry {
    use k8s_openapi::api::{
        apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet},
        autoscaling::v2::HorizontalPodAutoscaler,
        batch::v1::{CronJob, Job},
        core::v1::{
            ConfigMap, Endpoints, Event, LimitRange, Namespace, Node, PersistentVolume,
            PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
        },
        networking::v1::{Ingress, NetworkPolicy},
        policy::v1::PodDisruptionBudget,
        rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding},
        storage::v1::StorageClass,
    };
    use kube::Resource;

    let mut registry = ResourceRegistry::new();

    // Helper macro to add a resource type with aliases
    // Uses compile-time type info from k8s-openapi via kube::Resource trait
    // We explicitly specify namespaced since the Resource trait's Scope is an associated type
    macro_rules! add_resource {
        ($type:ty, namespaced, [$($alias:expr),* $(,)?]) => {{
            add_resource!(@inner $type, Scope::Namespaced, [$($alias),*])
        }};
        ($type:ty, cluster, [$($alias:expr),* $(,)?]) => {{
            add_resource!(@inner $type, Scope::Cluster, [$($alias),*])
        }};
        (@inner $type:ty, $scope:expr, [$($alias:expr),* $(,)?]) => {{
            let ar = ApiResource {
                group: <$type>::group(&()).to_string(),
                version: <$type>::version(&()).to_string(),
                api_version: <$type>::api_version(&()).to_string(),
                kind: <$type>::kind(&()).to_string(),
                plural: <$type>::plural(&()).to_string(),
            };
            let caps = ApiCapabilities {
                scope: $scope,
                subresources: vec![],
                operations: vec![],
            };
            let table_name = <$type>::plural(&()).to_string();
            let info = ResourceInfo {
                api_resource: ar,
                capabilities: caps,
                table_name: table_name.clone(),
                aliases: vec![$($alias.to_string()),*],
                is_core: true,
                group: <$type>::group(&()).to_string(),
                version: <$type>::version(&()).to_string(),
            };
            registry.add(info);
        }};
    }

    // Core API (v1) - namespaced resources
    add_resource!(Pod, namespaced, ["pod", "po"]);
    add_resource!(Service, namespaced, ["service", "svc"]);
    add_resource!(ConfigMap, namespaced, ["configmap", "cm"]);
    add_resource!(Secret, namespaced, ["secret"]);
    add_resource!(Event, namespaced, ["event", "ev"]);
    add_resource!(ServiceAccount, namespaced, ["serviceaccount", "sa"]);
    add_resource!(Endpoints, namespaced, ["endpoint", "ep"]);
    add_resource!(PersistentVolumeClaim, namespaced, ["persistentvolumeclaim", "pvc"]);
    add_resource!(ResourceQuota, namespaced, ["resourcequota", "quota"]);
    add_resource!(LimitRange, namespaced, ["limitrange", "limits"]);

    // Core API (v1) - cluster-scoped resources
    add_resource!(Node, cluster, ["node", "no"]);
    add_resource!(Namespace, cluster, ["namespace", "ns"]);
    add_resource!(PersistentVolume, cluster, ["persistentvolume", "pv"]);

    // Apps API (apps/v1)
    add_resource!(Deployment, namespaced, ["deployment", "deploy"]);
    add_resource!(StatefulSet, namespaced, ["statefulset", "sts"]);
    add_resource!(DaemonSet, namespaced, ["daemonset", "ds"]);
    add_resource!(ReplicaSet, namespaced, ["replicaset", "rs"]);

    // Batch API (batch/v1)
    add_resource!(Job, namespaced, ["job"]);
    add_resource!(CronJob, namespaced, ["cronjob", "cj"]);

    // Networking API (networking.k8s.io/v1)
    add_resource!(Ingress, namespaced, ["ingress", "ing"]);
    add_resource!(NetworkPolicy, namespaced, ["networkpolicy", "netpol"]);

    // Autoscaling API (autoscaling/v2)
    add_resource!(HorizontalPodAutoscaler, namespaced, ["horizontalpodautoscaler", "hpa"]);

    // Policy API (policy/v1)
    add_resource!(PodDisruptionBudget, namespaced, ["poddisruptionbudget", "pdb"]);

    // Storage API (storage.k8s.io/v1) - cluster-scoped
    add_resource!(StorageClass, cluster, ["storageclass", "sc"]);

    // RBAC API (rbac.authorization.k8s.io/v1) - namespaced
    add_resource!(Role, namespaced, ["role"]);
    add_resource!(RoleBinding, namespaced, ["rolebinding"]);

    // RBAC API (rbac.authorization.k8s.io/v1) - cluster-scoped
    add_resource!(ClusterRole, cluster, ["clusterrole"]);
    add_resource!(ClusterRoleBinding, cluster, ["clusterrolebinding"]);

    registry
}

/// Discover all available resources on a Kubernetes cluster (including CRDs)
/// 
/// Note: This function queries the discovery API which can be slow with many clusters.
/// For fast startup, use `build_core_registry()` which provides instant access to core K8s resources.
/// This function is kept for potential future CRD discovery support.
#[allow(dead_code)]
pub async fn discover_resources(client: &Client) -> Result<ResourceRegistry> {
    let mut registry = ResourceRegistry::new();

    // Run discovery
    let discovery = Discovery::new(client.clone()).run().await?;

    for group in discovery.groups() {
        // Get the preferred version's resources
        for (ar, caps) in group.recommended_resources() {
            // Skip subresources (e.g., pods/log, pods/exec)
            if ar.plural.contains('/') {
                continue;
            }

            // Determine table name (plural, lowercase)
            let table_name = ar.plural.to_lowercase();

            // Mark as core if from a standard K8s API group
            let is_core = matches!(
                ar.group.as_str(),
                "" | "apps"
                    | "batch"
                    | "networking.k8s.io"
                    | "policy"
                    | "rbac.authorization.k8s.io"
                    | "storage.k8s.io"
                    | "autoscaling"
                    | "coordination.k8s.io"
            );

            // Use lowercase kind as a basic alias
            let aliases = vec![ar.kind.to_lowercase()];

            let info = ResourceInfo {
                api_resource: ar.clone(),
                capabilities: caps.clone(),
                table_name,
                aliases,
                is_core,
                group: ar.group.clone(),
                version: ar.version.clone(),
            };

            registry.add(info);
        }
    }

    Ok(registry)
}

/// Generate a PostgreSQL-style schema for a discovered resource
/// All resources get the same schema: metadata columns + spec/status as JSONB
pub fn generate_schema(info: &ResourceInfo) -> Vec<ColumnDef> {
    let mut columns = vec![
        // Virtual column for cluster/context name
        ColumnDef {
            name: "_cluster".to_string(),
            data_type: "text".to_string(),
            json_path: None,
            description: "Kubernetes context/cluster name".to_string(),
        },
        // API version and kind - self-describing columns for CRD safety
        ColumnDef {
            name: "api_version".to_string(),
            data_type: "text".to_string(),
            json_path: Some("apiVersion".to_string()),
            description: "API version (e.g., v1, apps/v1, cert-manager.io/v1)".to_string(),
        },
        ColumnDef {
            name: "kind".to_string(),
            data_type: "text".to_string(),
            json_path: Some("kind".to_string()),
            description: "Resource kind (e.g., Pod, Deployment, Certificate)".to_string(),
        },
        // Common metadata columns
        ColumnDef {
            name: "name".to_string(),
            data_type: "text".to_string(),
            json_path: Some("metadata.name".to_string()),
            description: "Resource name".to_string(),
        },
        ColumnDef {
            name: "namespace".to_string(),
            data_type: "text".to_string(),
            json_path: Some("metadata.namespace".to_string()),
            description: "Namespace (null for cluster-scoped)".to_string(),
        },
        ColumnDef {
            name: "uid".to_string(),
            data_type: "text".to_string(),
            json_path: Some("metadata.uid".to_string()),
            description: "Unique identifier".to_string(),
        },
        ColumnDef {
            name: "created".to_string(),
            data_type: "timestamp".to_string(),
            json_path: Some("metadata.creationTimestamp".to_string()),
            description: "Creation timestamp".to_string(),
        },
        ColumnDef {
            name: "labels".to_string(),
            data_type: "jsonb".to_string(),
            json_path: Some("metadata.labels".to_string()),
            description: "Resource labels".to_string(),
        },
        ColumnDef {
            name: "annotations".to_string(),
            data_type: "jsonb".to_string(),
            json_path: Some("metadata.annotations".to_string()),
            description: "Resource annotations".to_string(),
        },
    ];

    // Add resource-specific columns based on type
    // ConfigMaps and Secrets use 'data' instead of spec/status
    match info.table_name.as_str() {
        "configmaps" => {
            columns.push(ColumnDef {
                name: "data".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("data".to_string()),
                description: "Configuration data key-value pairs".to_string(),
            });
        }
        "secrets" => {
            columns.push(ColumnDef {
                name: "type".to_string(),
                data_type: "text".to_string(),
                json_path: Some("type".to_string()),
                description: "Secret type (Opaque, kubernetes.io/tls, etc.)".to_string(),
            });
            columns.push(ColumnDef {
                name: "data".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("data".to_string()),
                description: "Secret data (base64 encoded values)".to_string(),
            });
        }
        "events" => {
            // Events have a flat structure, not spec/status
            columns.extend(vec![
                ColumnDef {
                    name: "type".to_string(),
                    data_type: "text".to_string(),
                    json_path: Some("type".to_string()),
                    description: "Event type (Normal, Warning)".to_string(),
                },
                ColumnDef {
                    name: "reason".to_string(),
                    data_type: "text".to_string(),
                    json_path: Some("reason".to_string()),
                    description: "Short reason for the event".to_string(),
                },
                ColumnDef {
                    name: "message".to_string(),
                    data_type: "text".to_string(),
                    json_path: Some("message".to_string()),
                    description: "Human-readable event message".to_string(),
                },
                ColumnDef {
                    name: "count".to_string(),
                    data_type: "integer".to_string(),
                    json_path: Some("count".to_string()),
                    description: "Number of times this event occurred".to_string(),
                },
                ColumnDef {
                    name: "first_timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    json_path: Some("firstTimestamp".to_string()),
                    description: "First time the event occurred".to_string(),
                },
                ColumnDef {
                    name: "last_timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    json_path: Some("lastTimestamp".to_string()),
                    description: "Last time the event occurred".to_string(),
                },
                ColumnDef {
                    name: "involved_object".to_string(),
                    data_type: "jsonb".to_string(),
                    json_path: Some("involvedObject".to_string()),
                    description: "Object this event is about".to_string(),
                },
                ColumnDef {
                    name: "source".to_string(),
                    data_type: "jsonb".to_string(),
                    json_path: Some("source".to_string()),
                    description: "Component that generated the event".to_string(),
                },
            ]);
        }
        "podmetrics" => {
            // PodMetrics from metrics.k8s.io - has containers with CPU/memory usage
            columns.extend(vec![
                ColumnDef {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    json_path: Some("timestamp".to_string()),
                    description: "Time when metrics were collected".to_string(),
                },
                ColumnDef {
                    name: "window".to_string(),
                    data_type: "text".to_string(),
                    json_path: Some("window".to_string()),
                    description: "Time window of the metrics sample".to_string(),
                },
                ColumnDef {
                    name: "containers".to_string(),
                    data_type: "jsonb".to_string(),
                    json_path: Some("containers".to_string()),
                    description: "Container metrics (cpu, memory usage per container)".to_string(),
                },
            ]);
        }
        "nodemetrics" => {
            // NodeMetrics from metrics.k8s.io - has node-level CPU/memory usage
            columns.extend(vec![
                ColumnDef {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    json_path: Some("timestamp".to_string()),
                    description: "Time when metrics were collected".to_string(),
                },
                ColumnDef {
                    name: "window".to_string(),
                    data_type: "text".to_string(),
                    json_path: Some("window".to_string()),
                    description: "Time window of the metrics sample".to_string(),
                },
                ColumnDef {
                    name: "usage".to_string(),
                    data_type: "jsonb".to_string(),
                    json_path: Some("usage".to_string()),
                    description: "Node resource usage (cpu, memory)".to_string(),
                },
            ]);
        }
        _ => {
            // Most resources have spec and status
            columns.push(ColumnDef {
                name: "spec".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("spec".to_string()),
                description: "Resource specification (desired state)".to_string(),
            });
            columns.push(ColumnDef {
                name: "status".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("status".to_string()),
                description: "Resource status (current state)".to_string(),
            });
        }
    }

    columns
}
