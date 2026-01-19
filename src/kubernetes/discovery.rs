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
use kube::api::ObjectList;
use kube::discovery::{ApiCapabilities, ApiResource, Scope};
use std::collections::{HashMap, HashSet};

/// Arrow data type for column schema
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnDataType {
    /// UTF-8 string (text, JSON blobs)
    Text,
    /// Millisecond timestamp
    Timestamp,
    /// 64-bit integer
    Integer,
}

/// Convert ColumnDataType to string representation (always lowercase)
impl std::fmt::Display for ColumnDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnDataType::Text => write!(f, "text"),
            ColumnDataType::Timestamp => write!(f, "timestamp"),
            ColumnDataType::Integer => write!(f, "integer"),
        }
    }
}

impl std::str::FromStr for ColumnDataType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(ColumnDataType::Text),
            "timestamp" => Ok(ColumnDataType::Timestamp),
            "integer" => Ok(ColumnDataType::Integer),
            _ => Err(anyhow::anyhow!("Unknown column data type: {}", s)),
        }
    }
}

/// Column definition for schema
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    /// Arrow data type
    pub data_type: ColumnDataType,
    /// JSON Pointer (RFC 6901) to extract value (e.g., "/metadata/name")
    pub json_path: Option<String>,
    /// Whether this column contains a JSON object/array that supports dot-notation access
    pub is_json_object: bool,
}

/// Default JSON object columns that support dot-notation access.
/// These are columns containing JSON objects/arrays from K8s resources.
/// Used by the JSON path preprocessor for syntax like `spec.containers[0].image`.
pub const DEFAULT_JSON_OBJECT_COLUMNS: &[&str] = &[
    "labels",           // metadata.labels - key-value map
    "annotations",      // metadata.annotations - key-value map
    "spec",             // resource specification - nested object
    "status",           // resource status - nested object
    "data",             // ConfigMap/Secret data - key-value map
    "owner_references", // metadata.ownerReferences - array of objects
];

impl ColumnDef {
    /// Create a text column with a JSON path
    fn text(name: &str, json_path: &str) -> Self {
        // Auto-detect if this is a JSON object column
        let is_json_object = DEFAULT_JSON_OBJECT_COLUMNS.contains(&name);
        Self {
            name: name.into(),
            data_type: ColumnDataType::Text,
            json_path: Some(json_path.into()),
            is_json_object,
        }
    }

    /// Create a JSON array/object column with a JSON path.
    /// Always sets is_json_object=true, enabling dot-notation and bracket syntax.
    /// Use this for columns that contain JSON arrays or objects (e.g., containers, rules, subjects).
    fn json(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Text,
            json_path: Some(json_path.into()),
            is_json_object: true,
        }
    }

    /// Create a text column without a JSON path (uses name as path)
    fn text_raw(name: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Text,
            json_path: None,
            is_json_object: false,
        }
    }

    /// Create a timestamp column with a JSON path
    fn timestamp(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Timestamp,
            json_path: Some(json_path.into()),
            is_json_object: false,
        }
    }

    /// Create an integer column with a JSON path
    fn integer(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Integer,
            json_path: Some(json_path.into()),
            is_json_object: false,
        }
    }
}

/// Result of CRD schema detection
///
/// Contains extracted schema information and metadata from a CRD.
#[derive(Debug, Clone)]
pub struct SchemaResult {
    /// Schema fields from CRD OpenAPI definition (None if no schema defined)
    pub fields: Option<Vec<ColumnDef>>,
    /// Short names from CRD spec.names.shortNames
    pub short_names: Vec<String>,
    /// Plural name for the resource (table name)
    pub plural: String,
    /// Singular name from CRD spec.names.singular (if defined)
    pub singular: Option<String>,
    /// Resource scope (Namespaced or Cluster)
    pub scope: Scope,
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
    /// Custom fields extracted from CRD OpenAPI schema (None = use default/core mapping)
    pub custom_fields: Option<Vec<ColumnDef>>,
}

impl ResourceInfo {
    /// Check if this resource is namespace-scoped
    pub fn is_namespaced(&self) -> bool {
        self.capabilities.scope == Scope::Namespaced
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

        // Check if this table name is already taken
        if let Some(_existing) = self.by_table_name.get(&original_table_name) {
            // Core resources always take priority - rename incoming non-core to Kind
            // Note: core resources added first (build_core_registry), so incoming core
            // with existing non-core shouldn't happen; both-core also shouldn't happen
            if !info.is_core {
                let kind_name = info.api_resource.kind.to_lowercase();
                if self.by_table_name.contains_key(&kind_name) {
                    return; // Kind name also conflicts, skip
                }
                info.table_name = kind_name;
                info.aliases = vec![];
            }
            // else: both core - shouldn't happen, but later wins if it does
        }

        // Add fully qualified name (resource.group) as alias for disambiguation
        // e.g., "certificates.cert-manager.io" - use with quotes in SQL
        if !info.group.is_empty() {
            let fq_name = format!("{}.{}", info.table_name, info.group);
            if !info.aliases.contains(&fq_name) {
                info.aliases.push(fq_name);
            }
        }

        // Register aliases (but don't let aliases overwrite existing table names)
        for alias in &info.aliases {
            if !self.by_table_name.contains_key(alias) {
                self.alias_map
                    .insert(alias.clone(), info.table_name.clone());
            }
        }

        self.alias_map
            .insert(info.table_name.clone(), info.table_name.clone());
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

    /// Get JSON column names for a specific table.
    /// Returns column names where is_json_object is true (arrays and objects that support dot-notation).
    /// Returns None if the table is not found.
    pub fn get_json_columns(&self, table_name: &str) -> Option<HashSet<String>> {
        let info = self.get(table_name)?;
        let schema = generate_schema(info);
        let json_cols: HashSet<String> = schema
            .into_iter()
            .filter(|col| col.is_json_object)
            .map(|col| col.name)
            .collect();
        Some(json_cols)
    }

    /// Get JSON column names for multiple tables, merged into a single set.
    /// Useful when a query involves multiple tables (JOINs).
    pub fn get_json_columns_for_tables(&self, table_names: &[String]) -> HashSet<String> {
        let mut result = HashSet::new();
        for name in table_names {
            if let Some(cols) = self.get_json_columns(name) {
                result.extend(cols);
            }
        }
        result
    }
}

/// Result of analyzing an OpenAPI schema type
struct OpenApiTypeInfo {
    /// The Arrow column data type
    data_type: ColumnDataType,
    /// Whether this is a JSON object/array that supports dot-notation access
    is_json_object: bool,
}

/// Map OpenAPI schema type to column data type and JSON object flag
fn analyze_openapi_type(schema: &JSONSchemaProps) -> OpenApiTypeInfo {
    // Check format first for more specific types
    if let Some(format) = &schema.format {
        match format.as_str() {
            "date-time" => {
                return OpenApiTypeInfo {
                    data_type: ColumnDataType::Timestamp,
                    is_json_object: false,
                };
            }
            "int64" | "int32" => {
                return OpenApiTypeInfo {
                    data_type: ColumnDataType::Integer,
                    is_json_object: false,
                };
            }
            _ => {}
        }
    }

    // Check the type field
    if let Some(type_str) = &schema.type_ {
        match type_str.as_str() {
            "integer" => {
                return OpenApiTypeInfo {
                    data_type: ColumnDataType::Integer,
                    is_json_object: false,
                };
            }
            "object" | "array" => {
                // Objects and arrays are JSON and support dot-notation
                return OpenApiTypeInfo {
                    data_type: ColumnDataType::Text,
                    is_json_object: true,
                };
            }
            _ => {}
        }
    }

    // Default to text (simple strings)
    OpenApiTypeInfo {
        data_type: ColumnDataType::Text,
        is_json_object: false,
    }
}

/// Extract column definitions from a CRD's OpenAPI v3 schema
fn extract_schema_fields(crd: &CustomResourceDefinition) -> Option<Vec<ColumnDef>> {
    // Find the stored/served version with a schema
    let version = crd.spec.versions.iter().find(|v| v.served && v.storage)?;

    let schema = version.schema.as_ref()?.open_api_v3_schema.as_ref()?;
    let properties = schema.properties.as_ref()?;

    let mut fields = Vec::new();

    for (name, prop) in properties {
        // Skip standard K8s fields that we handle separately
        if matches!(name.as_str(), "metadata" | "apiVersion" | "kind") {
            continue;
        }

        let type_info = analyze_openapi_type(prop);

        // Convert camelCase to snake_case for SQL column names
        let col_name = camel_to_snake(name);

        fields.push(ColumnDef {
            name: col_name,
            data_type: type_info.data_type,
            json_path: Some(format!("/{}", name)),
            is_json_object: type_info.is_json_object,
        });
    }

    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

/// Convert camelCase to snake_case
fn camel_to_snake(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }
    result
}

/// Find CRD schema from a pre-fetched CustomResourceDefinition list
///
/// Returns `Some(SchemaResult)` if the CRD exists, `None` if not found.
/// This is a synchronous search operation through the provided CRD list.
pub fn find_crd_schema(
    group: &str,
    kind: &str,
    crd_list: &ObjectList<CustomResourceDefinition>,
) -> Option<SchemaResult> {
    // Find the CRD that matches our group and kind
    let crd = crd_list
        .items
        .iter()
        .find(|crd| crd.spec.group == group && crd.spec.names.kind.eq_ignore_ascii_case(kind))?;

    // Extract short names from CRD spec.names.shortNames
    let short_names = crd
        .spec
        .names
        .short_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect();

    // Extract schema fields if available
    let fields = extract_schema_fields(crd);

    // Get plural, singular, and scope from CRD spec
    let plural = crd.spec.names.plural.clone();
    let singular = crd.spec.names.singular.clone();
    let scope = if crd.spec.scope == "Namespaced" {
        Scope::Namespaced
    } else {
        Scope::Cluster
    };

    Some(SchemaResult {
        fields,
        short_names,
        plural,
        singular,
        scope,
    })
}

/// Fetch CRD schemas for all non-core resources
///
/// Makes a single API call to list all CRDs, then searches the list for each resource.
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
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
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
                custom_fields: None, // Core resources use get_core_resource_fields()
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
    add_resource!(
        PersistentVolumeClaim,
        namespaced,
        ["persistentvolumeclaim", "pvc"]
    );
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
    add_resource!(
        HorizontalPodAutoscaler,
        namespaced,
        ["horizontalpodautoscaler", "hpa"]
    );

    // Policy API (policy/v1)
    add_resource!(
        PodDisruptionBudget,
        namespaced,
        ["poddisruptionbudget", "pdb"]
    );

    // Storage API (storage.k8s.io/v1) - cluster-scoped
    add_resource!(StorageClass, cluster, ["storageclass", "sc"]);

    // RBAC API (rbac.authorization.k8s.io/v1) - namespaced
    add_resource!(Role, namespaced, ["role"]);
    add_resource!(RoleBinding, namespaced, ["rolebinding"]);

    // RBAC API (rbac.authorization.k8s.io/v1) - cluster-scoped
    add_resource!(ClusterRole, cluster, ["clusterrole"]);
    add_resource!(ClusterRoleBinding, cluster, ["clusterrolebinding"]);

    // API Extensions (apiextensions.k8s.io/v1) - cluster-scoped
    add_resource!(
        CustomResourceDefinition,
        cluster,
        ["customresourcedefinition", "crd", "crds"]
    );

    // Metrics API (metrics.k8s.io/v1beta1) - optional, requires metrics-server
    // Note: k8s-metrics types have plural: "pods"/"nodes", but we need distinct table names
    // to avoid conflicts with core Pod/Node resources, so we construct ResourceInfo manually.
    {
        use k8s_metrics::v1beta1::{NodeMetrics, PodMetrics};
        use kube::Resource;

        // PodMetrics - namespaced
        let ar = ApiResource {
            group: PodMetrics::group(&()).to_string(),
            version: PodMetrics::version(&()).to_string(),
            api_version: PodMetrics::api_version(&()).to_string(),
            kind: PodMetrics::kind(&()).to_string(),
            plural: PodMetrics::plural(&()).to_string(), // "pods" - used for API calls
        };
        let info = ResourceInfo {
            api_resource: ar,
            capabilities: ApiCapabilities {
                scope: Scope::Namespaced,
                subresources: vec![],
                operations: vec![],
            },
            table_name: "podmetrics".to_string(), // Custom table name to avoid conflict
            aliases: vec!["podmetric".to_string()],
            is_core: true,
            group: PodMetrics::group(&()).to_string(),
            version: PodMetrics::version(&()).to_string(),
            custom_fields: None,
        };
        registry.add(info);

        // NodeMetrics - cluster-scoped
        let ar = ApiResource {
            group: NodeMetrics::group(&()).to_string(),
            version: NodeMetrics::version(&()).to_string(),
            api_version: NodeMetrics::api_version(&()).to_string(),
            kind: NodeMetrics::kind(&()).to_string(),
            plural: NodeMetrics::plural(&()).to_string(), // "nodes" - used for API calls
        };
        let info = ResourceInfo {
            api_resource: ar,
            capabilities: ApiCapabilities {
                scope: Scope::Cluster,
                subresources: vec![],
                operations: vec![],
            },
            table_name: "nodemetrics".to_string(), // Custom table name to avoid conflict
            aliases: vec!["nodemetric".to_string()],
            is_core: true,
            group: NodeMetrics::group(&()).to_string(),
            version: NodeMetrics::version(&()).to_string(),
            custom_fields: None,
        };
        registry.add(info);
    }

    registry
}

/// Get resource-specific columns for core Kubernetes resources.
///
/// This function provides explicit field mappings for all core K8s resources,
/// based on the actual structure defined in k8s-openapi. Resources that don't
/// follow the standard spec/status pattern get their actual top-level fields.
///
/// Returns None for unknown resources (CRDs), which should use schema detection.
fn get_core_resource_fields(table_name: &str) -> Option<Vec<ColumnDef>> {
    match table_name {
        // ==================== Standard spec/status pattern ====================
        // Most workload and configuration resources follow this pattern
        "pods"
        | "deployments"
        | "statefulsets"
        | "daemonsets"
        | "replicasets"
        | "jobs"
        | "cronjobs"
        | "services"
        | "ingresses"
        | "networkpolicies"
        | "persistentvolumeclaims"
        | "persistentvolumes"
        | "storageclasses"
        | "horizontalpodautoscalers"
        | "poddisruptionbudgets"
        | "namespaces"
        | "nodes"
        | "resourcequotas"
        | "limitranges"
        | "leases" => Some(vec![
            ColumnDef::text("spec", "/spec"),
            ColumnDef::text("status", "/status"),
        ]),

        // ==================== RBAC: rules pattern ====================
        // Role and ClusterRole have rules array, not spec/status
        "roles" | "clusterroles" => Some(vec![
            ColumnDef::json("rules", "/rules"), // array of PolicyRule
            ColumnDef::json("aggregation_rule", "/aggregationRule"), // AggregationRule object
        ]),

        // ==================== RBAC: binding pattern ====================
        // RoleBinding and ClusterRoleBinding reference a role and subjects
        "rolebindings" | "clusterrolebindings" => Some(vec![
            ColumnDef::json("role_ref", "/roleRef"),  // RoleRef object
            ColumnDef::json("subjects", "/subjects"), // array of Subject
        ]),

        // ==================== ServiceAccount: flat fields ====================
        "serviceaccounts" => Some(vec![
            ColumnDef::json("secrets", "/secrets"), // array of ObjectReference
            ColumnDef::json("image_pull_secrets", "/imagePullSecrets"), // array of LocalObjectReference
            ColumnDef::text(
                "automount_service_account_token",
                "/automountServiceAccountToken",
            ), // boolean (scalar)
        ]),

        // ==================== Endpoints: subsets pattern ====================
        "endpoints" => Some(vec![ColumnDef::json("subsets", "/subsets")]), // array of EndpointSubset

        // ==================== ConfigMap/Secret: data pattern ====================
        "configmaps" => Some(vec![
            ColumnDef::text("data", "/data"), // handled by DEFAULT_JSON_OBJECT_COLUMNS
            ColumnDef::json("binary_data", "/binaryData"), // map[string][]byte
            ColumnDef::text("immutable", "/immutable"), // boolean (scalar)
        ]),
        "secrets" => Some(vec![
            ColumnDef::text("type", "/type"),              // string (scalar)
            ColumnDef::text("data", "/data"),              // handled by DEFAULT_JSON_OBJECT_COLUMNS
            ColumnDef::json("string_data", "/stringData"), // map[string]string
            ColumnDef::text("immutable", "/immutable"),    // boolean (scalar)
        ]),

        // ==================== Events: flat structure ====================
        "events" => Some(vec![
            ColumnDef::text("type", "/type"),       // string (scalar)
            ColumnDef::text("reason", "/reason"),   // string (scalar)
            ColumnDef::text("message", "/message"), // string (scalar)
            ColumnDef::integer("count", "/count"),
            ColumnDef::timestamp("first_timestamp", "/firstTimestamp"),
            ColumnDef::timestamp("last_timestamp", "/lastTimestamp"),
            ColumnDef::json("involved_object", "/involvedObject"), // ObjectReference
            ColumnDef::json("source", "/source"),                  // EventSource object
        ]),

        // ==================== Metrics: special structure ====================
        "podmetrics" => Some(vec![
            ColumnDef::timestamp("timestamp", "/timestamp"),
            ColumnDef::text("window", "/window"), // string (duration)
            ColumnDef::json("containers", "/containers"), // array of ContainerMetrics
        ]),
        "nodemetrics" => Some(vec![
            ColumnDef::timestamp("timestamp", "/timestamp"),
            ColumnDef::text("window", "/window"), // string (duration)
            ColumnDef::json("usage", "/usage"),   // ResourceList (map)
        ]),

        // ==================== CustomResourceDefinitions: CRD metadata ====================
        "customresourcedefinitions" => Some(vec![
            ColumnDef::text("group", "/spec/group"), // string (scalar)
            ColumnDef::text("scope", "/spec/scope"), // string (scalar)
            ColumnDef::text("resource_kind", "/spec/names/kind"), // string (scalar)
            ColumnDef::text("plural", "/spec/names/plural"), // string (scalar)
            ColumnDef::text("singular", "/spec/names/singular"), // string (scalar)
            ColumnDef::json("short_names", "/spec/names/shortNames"), // array of strings
            ColumnDef::json("categories", "/spec/names/categories"), // array of strings
                                                     // Note: spec.versions and status.conditions are too large for table display
                                                     // Use kubectl get crd <name> -o yaml for full schema details
        ]),

        // Unknown resource - return None to trigger CRD schema detection or fallback
        _ => None,
    }
}

/// Generate a PostgreSQL-style schema for a discovered resource.
///
/// Schema generation follows this priority:
/// 1. Core resource explicit mapping (get_core_resource_fields)
/// 2. CRD schema from OpenAPI definition (custom_fields from fetch_crd_schemas)
/// 3. Fallback to spec/status pattern
pub fn generate_schema(info: &ResourceInfo) -> Vec<ColumnDef> {
    let mut columns = vec![
        // Virtual column for cluster/context name
        ColumnDef::text_raw("_cluster"),
        // API version and kind - self-describing columns for CRD safety
        ColumnDef::text("api_version", "/apiVersion"),
        ColumnDef::text("kind", "/kind"),
        // Common metadata columns
        ColumnDef::text("name", "/metadata/name"),
    ];

    // Only add namespace for namespaced resources (exclude cluster-scoped)
    if info.capabilities.scope == Scope::Namespaced {
        columns.push(ColumnDef::text("namespace", "/metadata/namespace"));
    }

    // Continue with remaining metadata columns
    columns.extend(vec![
        ColumnDef::text("uid", "/metadata/uid"),
        ColumnDef::timestamp("created", "/metadata/creationTimestamp"),
        ColumnDef::text("labels", "/metadata/labels"),
        ColumnDef::text("annotations", "/metadata/annotations"),
        ColumnDef::text("owner_references", "/metadata/ownerReferences"),
        ColumnDef::integer("generation", "/metadata/generation"),
        ColumnDef::text("resource_version", "/metadata/resourceVersion"),
        ColumnDef::timestamp("deletion_timestamp", "/metadata/deletionTimestamp"),
        ColumnDef::text("finalizers", "/metadata/finalizers"),
    ]);

    // Add resource-specific columns with priority:
    // 1. Core resource explicit mapping
    // 2. CRD schema from OpenAPI definition (cached in custom_fields)
    // 3. Fallback to spec/status
    if let Some(fields) = get_core_resource_fields(&info.table_name) {
        // Core resource with known field mapping
        columns.extend(fields);
    } else if let Some(fields) = &info.custom_fields {
        // CRD with schema extracted from OpenAPI definition
        columns.extend(fields.clone());
    } else {
        // Unknown resource or CRD without schema - fall back to spec/status
        columns.push(ColumnDef::text("spec", "/spec"));
        columns.push(ColumnDef::text("status", "/status"));
    }

    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_column_data_type_display() {
        assert_eq!(ColumnDataType::Text.to_string(), "text");
        assert_eq!(ColumnDataType::Timestamp.to_string(), "timestamp");
        assert_eq!(ColumnDataType::Integer.to_string(), "integer");
    }

    #[test]
    fn test_column_data_type_from_str() {
        assert_eq!(
            ColumnDataType::from_str("text").unwrap(),
            ColumnDataType::Text
        );
        assert_eq!(
            ColumnDataType::from_str("timestamp").unwrap(),
            ColumnDataType::Timestamp
        );
        assert_eq!(
            ColumnDataType::from_str("integer").unwrap(),
            ColumnDataType::Integer
        );
    }

    #[test]
    fn test_column_data_type_from_str_case_insensitive() {
        assert_eq!(
            ColumnDataType::from_str("TEXT").unwrap(),
            ColumnDataType::Text
        );
        assert_eq!(
            ColumnDataType::from_str("TimeStamp").unwrap(),
            ColumnDataType::Timestamp
        );
        assert_eq!(
            ColumnDataType::from_str("INTEGER").unwrap(),
            ColumnDataType::Integer
        );
    }

    #[test]
    fn test_column_data_type_from_str_invalid() {
        let result = ColumnDataType::from_str("invalid");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Unknown column data type")
        );
    }

    #[test]
    fn test_column_data_type_roundtrip() {
        // Test that Display + FromStr roundtrip correctly
        for data_type in [
            ColumnDataType::Text,
            ColumnDataType::Timestamp,
            ColumnDataType::Integer,
        ] {
            let string = data_type.to_string();
            let parsed = ColumnDataType::from_str(&string).unwrap();
            assert_eq!(parsed, data_type);
        }
    }
}
