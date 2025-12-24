// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Resource discovery for Kubernetes clusters.
//!
//! Discovers all available resources (including CRDs) at runtime using
//! the Kubernetes discovery API.

use anyhow::Result;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, JSONSchemaProps,
};
use kube::api::Api;
use kube::discovery::{ApiCapabilities, ApiResource, Discovery, Scope};
use kube::Client;
use schemars::schema::{InstanceType, Schema, SingleOrVec};
use schemars::JsonSchema;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Column definition for schema
#[derive(Debug, Clone)]
#[allow(dead_code)]
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
    /// Schema columns extracted from CRD (for non-core resources)
    pub crd_columns: Option<Vec<ColumnDef>>,
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

    // Fetch all CRDs for schema introspection
    let crd_schemas = fetch_crd_schemas(client).await.unwrap_or_default();

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

            // For non-core resources, look up CRD schema
            let crd_columns = if !is_core && !ar.group.is_empty() {
                let crd_name = format!("{}.{}", ar.plural, ar.group);
                crd_schemas.get(&crd_name).cloned()
            } else {
                None
            };

            let info = ResourceInfo {
                api_resource: ar.clone(),
                capabilities: caps.clone(),
                table_name,
                aliases,
                is_core,
                group: ar.group.clone(),
                version: ar.version.clone(),
                crd_columns,
            };

            registry.add(info);
        }
    }

    Ok(registry)
}

/// Fetch CRD definitions and extract their schemas
async fn fetch_crd_schemas(client: &Client) -> Result<HashMap<String, Vec<ColumnDef>>> {
    let crd_api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let crds = crd_api.list(&Default::default()).await?;

    let mut schemas = HashMap::new();

    for crd in crds {
        let crd_name = crd.metadata.name.clone().unwrap_or_default();

        // Get the schema from the first (or stored) version
        for version in &crd.spec.versions {
            if let Some(schema) = &version.schema {
                if let Some(props) = &schema.open_api_v3_schema {
                    let columns = extract_columns_from_schema(props);
                    if !columns.is_empty() {
                        schemas.insert(crd_name.clone(), columns);
                        break; // Use first version with schema
                    }
                }
            }
        }
    }

    Ok(schemas)
}

/// Extract column definitions from a JSONSchemaProps
fn extract_columns_from_schema(schema: &JSONSchemaProps) -> Vec<ColumnDef> {
    let mut columns = Vec::new();

    // Extract top-level properties (spec, status, etc.)
    if let Some(properties) = &schema.properties {
        // Process spec fields
        if let Some(spec_schema) = properties.get("spec") {
            extract_nested_columns(&mut columns, spec_schema, "spec", 2);
        }

        // Process status fields
        if let Some(status_schema) = properties.get("status") {
            extract_nested_columns(&mut columns, status_schema, "status", 2);
        }
    }

    columns
}

/// Reserved column names that we use for metadata
const RESERVED_COLUMNS: &[&str] = &[
    "_cluster",
    "name",
    "namespace",
    "uid",
    "created",
    "labels",
    "annotations",
    "spec",
    "status",
    "data",
    "type",
];

/// Extract columns from nested schema properties
fn extract_nested_columns(
    columns: &mut Vec<ColumnDef>,
    schema: &JSONSchemaProps,
    prefix: &str,
    max_depth: usize,
) {
    if max_depth == 0 {
        return;
    }

    if let Some(properties) = &schema.properties {
        for (name, prop) in properties {
            let json_path = format!("{}.{}", prefix, name);
            let data_type = json_schema_type_to_sql(prop);
            let description = prop.description.clone().unwrap_or_default();

            // Skip reserved column names to avoid conflicts with metadata columns
            if RESERVED_COLUMNS.contains(&name.as_str()) {
                continue;
            }

            // For simple types, add as column
            if is_simple_type(prop) {
                columns.push(ColumnDef {
                    name: name.clone(),
                    data_type,
                    json_path: Some(json_path),
                    description,
                });
            } else if prop.type_.as_deref() == Some("object") && max_depth > 1 {
                // For nested objects, recurse but with limited depth
                extract_nested_columns(columns, prop, &json_path, max_depth - 1);
            }
        }
    }
}

/// Check if a schema type is simple (not nested object/array)
fn is_simple_type(schema: &JSONSchemaProps) -> bool {
    match schema.type_.as_deref() {
        Some("string") | Some("integer") | Some("number") | Some("boolean") => true,
        Some("array") => {
            // Arrays of simple types are ok
            if let Some(items) = &schema.items {
                match items {
                    k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::JSONSchemaPropsOrArray::Schema(item_schema) => {
                        matches!(item_schema.type_.as_deref(), Some("string") | Some("integer") | Some("number") | Some("boolean"))
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        _ => false,
    }
}

/// Convert JSON Schema type to SQL type
fn json_schema_type_to_sql(schema: &JSONSchemaProps) -> String {
    match schema.type_.as_deref() {
        Some("string") => {
            // Check format for more specific types
            match schema.format.as_deref() {
                Some("date-time") => "timestamp".to_string(),
                Some("date") => "date".to_string(),
                _ => "text".to_string(),
            }
        }
        Some("integer") => "integer".to_string(),
        Some("number") => "numeric".to_string(),
        Some("boolean") => "boolean".to_string(),
        Some("array") => "jsonb".to_string(),
        Some("object") => "jsonb".to_string(),
        _ => "text".to_string(),
    }
}

/// Generate columns from a type implementing JsonSchema
fn columns_from_jsonschema<T: JsonSchema>() -> Vec<ColumnDef> {
    let root_schema = schemars::schema_for!(T);
    let mut columns = Vec::new();
    let mut seen_names = HashSet::new();

    // The root might be a reference, resolve it first
    let resolved = if let Some(ref_path) = &root_schema.schema.reference {
        let def_name = ref_path.strip_prefix("#/definitions/").unwrap_or(ref_path);
        root_schema.definitions.get(def_name)
    } else {
        Some(&Schema::Object(root_schema.schema.clone()))
    };

    let Some(Schema::Object(schema)) = resolved else {
        return columns;
    };

    // Get the root schema object
    if let Some(object) = &schema.object {
        // Process spec and status subschemas
        let props = &object.properties;
        if let Some(spec) = props.get("spec") {
            extract_schemars_columns(&mut columns, &mut seen_names, spec, "spec", 2, &root_schema.definitions);
        }
        if let Some(status) = props.get("status") {
            extract_schemars_columns(&mut columns, &mut seen_names, status, "status", 2, &root_schema.definitions);
        }
        // For ConfigMaps/Secrets, also look at "data" and "type"
        if let Some(data) = props.get("data") {
            extract_schemars_columns(&mut columns, &mut seen_names, data, "data", 1, &root_schema.definitions);
        }
    }

    columns
}

/// Extract columns from a schemars Schema
fn extract_schemars_columns(
    columns: &mut Vec<ColumnDef>,
    seen_names: &mut HashSet<String>,
    schema: &Schema,
    prefix: &str,
    max_depth: usize,
    definitions: &BTreeMap<String, Schema>,
) {
    if max_depth == 0 {
        return;
    }

    // Resolve references first
    let resolved = resolve_schema_ref(schema, definitions);

    let Some(obj) = resolved.as_ref().and_then(|s| match s {
        Schema::Object(o) => Some(o),
        _ => None,
    }) else {
        return;
    };

    if let Some(object) = &obj.object {
        let props = &object.properties;
        for (name, prop) in props {
            let json_path = format!("{}.{}", prefix, name);

            // Skip reserved column names
            if RESERVED_COLUMNS.contains(&name.as_str()) {
                continue;
            }

            // Use prefixed name if there's a conflict
            let column_name = if seen_names.contains(name) {
                format!("{}_{}", prefix, name)
            } else {
                name.clone()
            };
            seen_names.insert(name.clone());

            let data_type = schemars_type_to_sql(prop, definitions);
            let description = get_schemars_description(prop, definitions);

            // Add all properties as columns - simple types get their native type,
            // complex types (objects, arrays) become jsonb
            columns.push(ColumnDef {
                name: column_name,
                data_type,
                json_path: Some(json_path),
                description,
            });
        }
    }
}

/// Resolve a schema reference if present
/// Handles both direct $ref and allOf/anyOf/oneOf with a single $ref item
fn resolve_schema_ref<'a>(
    schema: &'a Schema,
    definitions: &'a BTreeMap<String, Schema>,
) -> Option<&'a Schema> {
    match schema {
        Schema::Object(obj) => {
            // Direct reference
            if let Some(ref_path) = &obj.reference {
                let def_name = ref_path.strip_prefix("#/definitions/")?;
                return definitions.get(def_name);
            }

            // Check for allOf with a single reference (common pattern in k8s schemas)
            if let Some(subschemas) = &obj.subschemas {
                if let Some(all_of) = &subschemas.all_of {
                    if all_of.len() == 1 {
                        if let Schema::Object(inner) = &all_of[0] {
                            if let Some(ref_path) = &inner.reference {
                                let def_name = ref_path.strip_prefix("#/definitions/")?;
                                return definitions.get(def_name);
                            }
                        }
                    }
                }
                // Also check anyOf and oneOf
                if let Some(any_of) = &subschemas.any_of {
                    if any_of.len() == 1 {
                        if let Schema::Object(inner) = &any_of[0] {
                            if let Some(ref_path) = &inner.reference {
                                let def_name = ref_path.strip_prefix("#/definitions/")?;
                                return definitions.get(def_name);
                            }
                        }
                    }
                }
            }

            Some(schema)
        }
        _ => Some(schema),
    }
}

/// Convert schemars type to SQL type
fn schemars_type_to_sql(schema: &Schema, definitions: &BTreeMap<String, Schema>) -> String {
    let resolved = resolve_schema_ref(schema, definitions);
    let Some(obj) = resolved.and_then(|s| match s {
        Schema::Object(o) => Some(o),
        _ => None,
    }) else {
        return "text".to_string();
    };

    match &obj.instance_type {
        Some(SingleOrVec::Single(t)) => match **t {
            InstanceType::String => {
                if let Some(format) = &obj.format {
                    match format.as_str() {
                        "date-time" => "timestamp".to_string(),
                        "date" => "date".to_string(),
                        _ => "text".to_string(),
                    }
                } else {
                    "text".to_string()
                }
            }
            InstanceType::Integer => "integer".to_string(),
            InstanceType::Number => "numeric".to_string(),
            InstanceType::Boolean => "boolean".to_string(),
            InstanceType::Array => "jsonb".to_string(),
            InstanceType::Object => "jsonb".to_string(),
            _ => "text".to_string(),
        },
        _ => "text".to_string(),
    }
}

/// Get description from schemars schema
fn get_schemars_description(
    schema: &Schema,
    definitions: &BTreeMap<String, Schema>,
) -> String {
    let resolved = resolve_schema_ref(schema, definitions);
    resolved
        .and_then(|s| match s {
            Schema::Object(o) => o.metadata.as_ref().and_then(|m| m.description.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// Get columns for a core resource type using JsonSchema
pub fn get_core_type_columns(table_name: &str) -> Vec<ColumnDef> {
    use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
    use k8s_openapi::api::batch::v1::{CronJob, Job};
    use k8s_openapi::api::core::v1::{
        ConfigMap, Endpoints, Event, LimitRange, Namespace, Node, PersistentVolume,
        PersistentVolumeClaim, Pod, ResourceQuota, Secret, Service, ServiceAccount,
    };
    use k8s_openapi::api::networking::v1::{Ingress, NetworkPolicy};
    use k8s_openapi::api::policy::v1::PodDisruptionBudget;
    use k8s_openapi::api::autoscaling::v2::HorizontalPodAutoscaler;
    use k8s_openapi::api::storage::v1::StorageClass;
    use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};

    match table_name {
        "pods" => columns_from_jsonschema::<Pod>(),
        "services" => columns_from_jsonschema::<Service>(),
        "deployments" => columns_from_jsonschema::<Deployment>(),
        "configmaps" => columns_from_jsonschema::<ConfigMap>(),
        "secrets" => columns_from_jsonschema::<Secret>(),
        "nodes" => columns_from_jsonschema::<Node>(),
        "namespaces" => columns_from_jsonschema::<Namespace>(),
        "ingresses" => columns_from_jsonschema::<Ingress>(),
        "jobs" => columns_from_jsonschema::<Job>(),
        "cronjobs" => columns_from_jsonschema::<CronJob>(),
        "statefulsets" => columns_from_jsonschema::<StatefulSet>(),
        "daemonsets" => columns_from_jsonschema::<DaemonSet>(),
        "replicasets" => columns_from_jsonschema::<ReplicaSet>(),
        "persistentvolumeclaims" => columns_from_jsonschema::<PersistentVolumeClaim>(),
        "persistentvolumes" => columns_from_jsonschema::<PersistentVolume>(),
        "events" => columns_from_jsonschema::<Event>(),
        "serviceaccounts" => columns_from_jsonschema::<ServiceAccount>(),
        "endpoints" => columns_from_jsonschema::<Endpoints>(),
        "resourcequotas" => columns_from_jsonschema::<ResourceQuota>(),
        "limitranges" => columns_from_jsonschema::<LimitRange>(),
        "horizontalpodautoscalers" => columns_from_jsonschema::<HorizontalPodAutoscaler>(),
        "poddisruptionbudgets" => columns_from_jsonschema::<PodDisruptionBudget>(),
        "networkpolicies" => columns_from_jsonschema::<NetworkPolicy>(),
        "storageclasses" => columns_from_jsonschema::<StorageClass>(),
        "roles" => columns_from_jsonschema::<Role>(),
        "rolebindings" => columns_from_jsonschema::<RoleBinding>(),
        "clusterroles" => columns_from_jsonschema::<ClusterRole>(),
        "clusterrolebindings" => columns_from_jsonschema::<ClusterRoleBinding>(),
        _ => vec![],
    }
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

    // For core resources, generate columns from JsonSchema
    if info.is_core {
        let extra = get_core_type_columns(&info.table_name);
        columns.extend(extra);
    } else if let Some(crd_cols) = &info.crd_columns {
        // For CRDs, add columns extracted from the schema
        columns.extend(crd_cols.clone());
    }

    // Add spec/status or data columns based on resource type
    // ConfigMaps and Secrets don't have spec/status
    match info.table_name.as_str() {
        "configmaps" => {
            columns.push(ColumnDef {
                name: "data".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("data".to_string()),
                description: "Configuration data".to_string(),
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
                description: "Secret data (base64 encoded)".to_string(),
            });
        }
        _ => {
            // Most resources have spec and status
            columns.push(ColumnDef {
                name: "spec".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("spec".to_string()),
                description: "Resource specification".to_string(),
            });
            columns.push(ColumnDef {
                name: "status".to_string(),
                data_type: "jsonb".to_string(),
                json_path: Some("status".to_string()),
                description: "Resource status".to_string(),
            });
        }
    }

    columns
}
