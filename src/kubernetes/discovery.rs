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

/// Column definition for schema
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub json_path: Option<String>,
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
