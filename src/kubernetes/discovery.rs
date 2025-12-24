// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Resource discovery for Kubernetes clusters.
//!
//! Discovers all available resources (including CRDs) at runtime using
//! the Kubernetes discovery API.

use anyhow::Result;
use kube::discovery::{ApiCapabilities, ApiResource, Discovery, Scope};
use kube::Client;
use std::collections::HashMap;

/// Information about a discovered Kubernetes resource
#[derive(Debug, Clone)]
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
    pub fn add(&mut self, info: ResourceInfo) {
        // Add aliases
        for alias in &info.aliases {
            self.alias_map.insert(alias.clone(), info.table_name.clone());
        }
        // Add the table name itself as an alias
        self.alias_map.insert(info.table_name.clone(), info.table_name.clone());
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
}

/// Core resources that we have static types for
/// These get priority handling and optimized schemas
const CORE_RESOURCES: &[(&str, &[&str])] = &[
    ("pods", &["pod"]),
    ("services", &["service", "svc"]),
    ("deployments", &["deployment", "deploy"]),
    ("configmaps", &["configmap", "cm"]),
    ("secrets", &["secret"]),
    ("nodes", &["node"]),
    ("namespaces", &["namespace", "ns"]),
    ("ingresses", &["ingress", "ing"]),
    ("jobs", &["job"]),
    ("cronjobs", &["cronjob", "cj"]),
    ("statefulsets", &["statefulset", "sts"]),
    ("daemonsets", &["daemonset", "ds"]),
    ("persistentvolumeclaims", &["persistentvolumeclaim", "pvc", "pvcs"]),
    ("persistentvolumes", &["persistentvolume", "pv", "pvs"]),
    ("replicasets", &["replicaset", "rs"]),
    ("events", &["event", "ev"]),
    ("serviceaccounts", &["serviceaccount", "sa"]),
    ("endpoints", &["endpoint", "ep"]),
    ("resourcequotas", &["resourcequota", "quota"]),
    ("limitranges", &["limitrange", "limits"]),
    ("horizontalpodautoscalers", &["horizontalpodautoscaler", "hpa"]),
    ("poddisruptionbudgets", &["poddisruptionbudget", "pdb"]),
    ("networkpolicies", &["networkpolicy", "netpol"]),
    ("storageclasses", &["storageclass", "sc"]),
    ("roles", &["role"]),
    ("rolebindings", &["rolebinding"]),
    ("clusterroles", &["clusterrole"]),
    ("clusterrolebindings", &["clusterrolebinding"]),
];

/// Discover all available resources on a Kubernetes cluster
pub async fn discover_resources(client: &Client) -> Result<ResourceRegistry> {
    let mut registry = ResourceRegistry::new();

    // Build a set of known core resource names for quick lookup
    let core_names: HashMap<&str, &[&str]> = CORE_RESOURCES.iter().cloned().collect();

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

            // Check if this is a core resource we know about
            let (is_core, aliases) = if let Some(known_aliases) = core_names.get(table_name.as_str()) {
                (true, known_aliases.iter().map(|s| s.to_string()).collect())
            } else {
                // For discovered resources, use kind as alias
                let aliases = vec![ar.kind.to_lowercase()];
                (false, aliases)
            };

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
/// Core resources get detailed schemas, CRDs get generic metadata + JSON blobs
pub fn generate_schema(info: &ResourceInfo) -> Vec<ColumnDef> {
    let mut columns = vec![
        // Every resource has _cluster as first column
        ColumnDef {
            name: "_cluster".to_string(),
            data_type: "text".to_string(),
            json_path: None,
            description: "Kubernetes context/cluster name".to_string(),
        },
    ];

    // Common metadata columns for all resources
    columns.extend(vec![
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
    ]);

    // For non-namespaced resources, still include namespace column but it will be null
    // This keeps schema consistent

    // Add spec and status as JSONB columns
    columns.extend(vec![
        ColumnDef {
            name: "spec".to_string(),
            data_type: "jsonb".to_string(),
            json_path: Some("spec".to_string()),
            description: "Resource specification".to_string(),
        },
        ColumnDef {
            name: "status".to_string(),
            data_type: "jsonb".to_string(),
            json_path: Some("status".to_string()),
            description: "Resource status".to_string(),
        },
    ]);

    // For core resources, add type-specific columns
    if info.is_core {
        let extra = get_core_resource_columns(&info.table_name);
        // Insert before spec/status
        let insert_pos = columns.len() - 2;
        for col in extra.into_iter().rev() {
            columns.insert(insert_pos, col);
        }
    }

    columns
}

/// Column definition for schema
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub json_path: Option<String>,
    pub description: String,
}

/// Get additional columns for core resources
fn get_core_resource_columns(table_name: &str) -> Vec<ColumnDef> {
    match table_name {
        "pods" => vec![
            ColumnDef {
                name: "phase".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.phase".to_string()),
                description: "Pod phase (Pending, Running, Succeeded, Failed, Unknown)".to_string(),
            },
            ColumnDef {
                name: "node".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.nodeName".to_string()),
                description: "Node the pod is scheduled on".to_string(),
            },
            ColumnDef {
                name: "ip".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.podIP".to_string()),
                description: "Pod IP address".to_string(),
            },
            ColumnDef {
                name: "restarts".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.containerStatuses[0].restartCount".to_string()),
                description: "Container restart count".to_string(),
            },
        ],
        "deployments" => vec![
            ColumnDef {
                name: "replicas".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("spec.replicas".to_string()),
                description: "Desired replicas".to_string(),
            },
            ColumnDef {
                name: "ready".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.readyReplicas".to_string()),
                description: "Ready replicas".to_string(),
            },
            ColumnDef {
                name: "available".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.availableReplicas".to_string()),
                description: "Available replicas".to_string(),
            },
        ],
        "services" => vec![
            ColumnDef {
                name: "type".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.type".to_string()),
                description: "Service type (ClusterIP, NodePort, LoadBalancer)".to_string(),
            },
            ColumnDef {
                name: "cluster_ip".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.clusterIP".to_string()),
                description: "Cluster IP address".to_string(),
            },
            ColumnDef {
                name: "external_ip".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.loadBalancer.ingress[0].ip".to_string()),
                description: "External IP (for LoadBalancer)".to_string(),
            },
            ColumnDef {
                name: "ports".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("spec.ports".to_string()),
                description: "Service ports".to_string(),
            },
        ],
        "nodes" => vec![
            ColumnDef {
                name: "ready".to_string(),
                data_type: "boolean".to_string(),
                json_path: Some("status.conditions".to_string()), // Special handling needed
                description: "Node ready status".to_string(),
            },
            ColumnDef {
                name: "version".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.nodeInfo.kubeletVersion".to_string()),
                description: "Kubelet version".to_string(),
            },
            ColumnDef {
                name: "os".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.nodeInfo.osImage".to_string()),
                description: "OS image".to_string(),
            },
            ColumnDef {
                name: "arch".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.nodeInfo.architecture".to_string()),
                description: "CPU architecture".to_string(),
            },
        ],
        "statefulsets" | "daemonsets" | "replicasets" => vec![
            ColumnDef {
                name: "replicas".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("spec.replicas".to_string()),
                description: "Desired replicas".to_string(),
            },
            ColumnDef {
                name: "ready".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.readyReplicas".to_string()),
                description: "Ready replicas".to_string(),
            },
        ],
        "jobs" => vec![
            ColumnDef {
                name: "completions".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("spec.completions".to_string()),
                description: "Desired completions".to_string(),
            },
            ColumnDef {
                name: "succeeded".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.succeeded".to_string()),
                description: "Succeeded pods".to_string(),
            },
            ColumnDef {
                name: "failed".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("status.failed".to_string()),
                description: "Failed pods".to_string(),
            },
        ],
        "cronjobs" => vec![
            ColumnDef {
                name: "schedule".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.schedule".to_string()),
                description: "Cron schedule".to_string(),
            },
            ColumnDef {
                name: "suspend".to_string(),
                data_type: "boolean".to_string(),
                json_path: Some("spec.suspend".to_string()),
                description: "Whether cronjob is suspended".to_string(),
            },
            ColumnDef {
                name: "last_schedule".to_string(),
                data_type: "timestamp".to_string(),
                json_path: Some("status.lastScheduleTime".to_string()),
                description: "Last schedule time".to_string(),
            },
        ],
        "persistentvolumeclaims" => vec![
            ColumnDef {
                name: "phase".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.phase".to_string()),
                description: "PVC phase (Pending, Bound, Lost)".to_string(),
            },
            ColumnDef {
                name: "storage_class".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.storageClassName".to_string()),
                description: "Storage class name".to_string(),
            },
            ColumnDef {
                name: "capacity".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.capacity.storage".to_string()),
                description: "Actual capacity".to_string(),
            },
            ColumnDef {
                name: "volume".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.volumeName".to_string()),
                description: "Bound volume name".to_string(),
            },
        ],
        "persistentvolumes" => vec![
            ColumnDef {
                name: "phase".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.phase".to_string()),
                description: "PV phase".to_string(),
            },
            ColumnDef {
                name: "storage_class".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.storageClassName".to_string()),
                description: "Storage class name".to_string(),
            },
            ColumnDef {
                name: "capacity".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.capacity.storage".to_string()),
                description: "Storage capacity".to_string(),
            },
            ColumnDef {
                name: "claim".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.claimRef.name".to_string()),
                description: "Bound claim name".to_string(),
            },
        ],
        "ingresses" => vec![
            ColumnDef {
                name: "class".to_string(),
                data_type: "text".to_string(),
                json_path: Some("spec.ingressClassName".to_string()),
                description: "Ingress class".to_string(),
            },
            ColumnDef {
                name: "hosts".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("spec.rules".to_string()),
                description: "Ingress rules/hosts".to_string(),
            },
            ColumnDef {
                name: "address".to_string(),
                data_type: "text".to_string(),
                json_path: Some("status.loadBalancer.ingress[0].ip".to_string()),
                description: "Load balancer address".to_string(),
            },
        ],
        "events" => vec![
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
                description: "Event reason".to_string(),
            },
            ColumnDef {
                name: "message".to_string(),
                data_type: "text".to_string(),
                json_path: Some("message".to_string()),
                description: "Event message".to_string(),
            },
            ColumnDef {
                name: "count".to_string(),
                data_type: "integer".to_string(),
                json_path: Some("count".to_string()),
                description: "Event count".to_string(),
            },
            ColumnDef {
                name: "source".to_string(),
                data_type: "text".to_string(),
                json_path: Some("source.component".to_string()),
                description: "Event source component".to_string(),
            },
        ],
        _ => vec![],
    }
}
