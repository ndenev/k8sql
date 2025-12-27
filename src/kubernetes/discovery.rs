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
use kube::Client;
use kube::api::Api;
use kube::discovery::{ApiCapabilities, ApiResource, Scope};
use std::collections::HashMap;

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

/// Column definition for schema
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    /// Arrow data type
    pub data_type: ColumnDataType,
    /// JSON path to extract value (e.g., "metadata.name")
    pub json_path: Option<String>,
}

impl ColumnDef {
    /// Create a text column with a JSON path
    fn text(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Text,
            json_path: Some(json_path.into()),
        }
    }

    /// Create a text column without a JSON path (uses name as path)
    fn text_raw(name: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Text,
            json_path: None,
        }
    }

    /// Create a timestamp column with a JSON path
    fn timestamp(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Timestamp,
            json_path: Some(json_path.into()),
        }
    }

    /// Create an integer column with a JSON path
    fn integer(name: &str, json_path: &str) -> Self {
        Self {
            name: name.into(),
            data_type: ColumnDataType::Integer,
            json_path: Some(json_path.into()),
        }
    }
}

/// Result of CRD schema detection
///
/// Distinguishes between successful schema extraction, legitimate absence of schema,
/// and transient errors that shouldn't trigger silent fallback.
#[derive(Debug, Clone)]
pub enum SchemaResult {
    /// Successfully extracted CRD information
    Found {
        /// Schema fields from CRD OpenAPI definition (None if no schema defined)
        fields: Option<Vec<ColumnDef>>,
        /// Short names from CRD spec.names.shortNames
        short_names: Vec<String>,
    },
    /// Failed to fetch CRD - don't silently fall back, report the error
    FetchError(String),
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
}

/// Check if an API group is a core Kubernetes group (not a CRD)
///
/// Core groups are:
/// 1. Groups we have static k8s-openapi types for (apps, batch, etc.)
/// 2. All *.k8s.io groups (core K8s APIs like authorization.k8s.io, events.k8s.io)
pub fn is_core_api_group(group: &str) -> bool {
    // Empty string is core/v1
    if group.is_empty() {
        return true;
    }
    // All *.k8s.io groups are core Kubernetes APIs
    if group.ends_with(".k8s.io") {
        return true;
    }
    // These are the short names without .k8s.io suffix
    matches!(group, "apps" | "batch" | "policy" | "autoscaling")
}

/// Map OpenAPI schema type to column data type
fn openapi_type_to_column_type(schema: &JSONSchemaProps) -> ColumnDataType {
    // Check format first for more specific types
    if let Some(format) = &schema.format {
        match format.as_str() {
            "date-time" => return ColumnDataType::Timestamp,
            "int64" | "int32" => return ColumnDataType::Integer,
            _ => {}
        }
    }

    // Fall back to type
    if let Some(type_str) = &schema.type_
        && type_str.as_str() == "integer"
    {
        return ColumnDataType::Integer;
    }

    // Default to text (handles strings, objects, arrays as JSON)
    ColumnDataType::Text
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

        let col_type = openapi_type_to_column_type(prop);

        // Convert camelCase to snake_case for SQL column names
        let col_name = camel_to_snake(name);

        fields.push(ColumnDef {
            name: col_name,
            data_type: col_type,
            json_path: Some(name.clone()),
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

/// Fetch CRD schema from the CustomResourceDefinition object
///
/// Returns:
/// - `SchemaResult::Fields` if schema was successfully extracted
/// - `SchemaResult::NoSchema` if CRD exists but has no schema (use fallback)
/// - `SchemaResult::FetchError` on transient errors (don't silently fall back)
pub async fn get_crd_schema(client: &Client, group: &str, kind: &str) -> SchemaResult {
    // CRD name is plural.group (e.g., "certificates.cert-manager.io")
    // But we receive the group and kind, so we need to construct the CRD name
    // The CRD name format is: <plural>.<group>
    // We'll try to fetch by listing CRDs and matching group + kind

    let api: Api<CustomResourceDefinition> = Api::all(client.clone());

    // List CRDs and find matching one by group and kind
    let crd_list = match api.list(&Default::default()).await {
        Ok(list) => list,
        Err(e) => {
            return SchemaResult::FetchError(format!(
                "Failed to list CRDs while looking for {}.{}: {}",
                kind, group, e
            ));
        }
    };

    // Find the CRD that matches our group and kind
    let crd = crd_list
        .items
        .into_iter()
        .find(|crd| crd.spec.group == group && crd.spec.names.kind.eq_ignore_ascii_case(kind));

    match crd {
        Some(crd) => {
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
            let fields = extract_schema_fields(&crd);

            SchemaResult::Found {
                fields,
                short_names,
            }
        }
        None => {
            // Not a CRD (or CRD not found) - return empty short names, no fields
            SchemaResult::Found {
                fields: None,
                short_names: Vec::new(),
            }
        }
    }
}

/// Fetch CRD schemas for all non-core resources in parallel
///
/// This function updates the `custom_fields` of each ResourceInfo with the
/// schema extracted from the CRD's OpenAPI definition.
pub async fn fetch_crd_schemas(client: &Client, resources: &mut [ResourceInfo]) -> Result<()> {
    use futures::future::join_all;

    // Filter to non-core resources that might be CRDs
    let crd_resources: Vec<_> = resources
        .iter()
        .enumerate()
        .filter(|(_, r)| !r.is_core && get_core_resource_fields(&r.table_name).is_none())
        .map(|(i, r)| (i, r.group.clone(), r.api_resource.kind.clone()))
        .collect();

    if crd_resources.is_empty() {
        return Ok(());
    }

    tracing::debug!(
        "Fetching schemas for {} potential CRDs",
        crd_resources.len()
    );

    // Fetch all CRD schemas in parallel
    let futures: Vec<_> = crd_resources
        .iter()
        .map(|(_, group, kind)| {
            let client = client.clone();
            let group = group.clone();
            let kind = kind.clone();
            async move { get_crd_schema(&client, &group, &kind).await }
        })
        .collect();

    let results = join_all(futures).await;

    // Update resources with fetched schemas and short names
    for ((idx, group, kind), result) in crd_resources.iter().zip(results) {
        match result {
            SchemaResult::Found {
                fields,
                short_names,
            } => {
                // Add short names to aliases
                if !short_names.is_empty() {
                    tracing::debug!(
                        "CRD {}.{}: adding short names {:?}",
                        kind,
                        group,
                        short_names
                    );
                    resources[*idx].aliases.extend(short_names);
                }

                // Set custom fields if schema was extracted
                match fields {
                    Some(f) => {
                        tracing::debug!(
                            "CRD {}.{}: extracted {} fields from schema",
                            kind,
                            group,
                            f.len()
                        );
                        resources[*idx].custom_fields = Some(f);
                    }
                    None => {
                        tracing::debug!(
                            "CRD {}.{}: no schema, using spec/status fallback",
                            kind,
                            group
                        );
                        // Leave custom_fields as None - generate_schema will use fallback
                    }
                }
            }
            SchemaResult::FetchError(e) => {
                tracing::warn!("CRD {}.{}: {}", kind, group, e);
                // Leave custom_fields as None - use fallback but log the warning
            }
        }
    }

    Ok(())
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

    registry
}

/// Get CRD API groups only (single API call)
/// Returns (group_name, version) pairs for non-core groups only
/// Core resources come from k8s-openapi at compile time, so we skip them
pub async fn get_crd_api_groups(client: &Client) -> Result<Vec<(String, String)>> {
    let t0 = std::time::Instant::now();
    let api_group_list = client.list_api_groups().await?;
    tracing::debug!(
        "get_crd_api_groups: list_api_groups {:?} ({} groups)",
        t0.elapsed(),
        api_group_list.groups.len()
    );

    let mut api_groups: Vec<(String, String)> = Vec::new();
    for group in &api_group_list.groups {
        // Skip core K8s API groups - we have static types for these
        if is_core_api_group(&group.name) {
            continue;
        }

        if let Some(pref) = group.preferred_version.as_ref() {
            api_groups.push((group.name.clone(), pref.version.clone()));
        } else if let Some(first) = group.versions.first() {
            api_groups.push((group.name.clone(), first.version.clone()));
        }
    }

    Ok(api_groups)
}

/// Discover resources for specific API groups only (parallel)
/// Returns a map of (group, version) -> Vec<ResourceInfo>
pub async fn discover_groups(
    client: &Client,
    groups: &[(String, String)],
) -> Result<std::collections::HashMap<(String, String), Vec<ResourceInfo>>> {
    use futures::future::join_all;
    use kube::discovery::oneshot;

    let t0 = std::time::Instant::now();
    let num_groups = groups.len();
    let futures: Vec<_> = groups
        .iter()
        .map(|(group_name, version)| {
            let client = client.clone();
            let group = group_name.clone();
            let ver = version.clone();
            async move {
                let t = std::time::Instant::now();
                let result = oneshot::group(&client, &group).await;
                ((group, ver), result, t.elapsed())
            }
        })
        .collect();

    let results = join_all(futures).await;
    tracing::debug!("discover_groups x{}: {:?} total", num_groups, t0.elapsed());

    // Log slowest groups
    let mut timings: Vec<_> = results
        .iter()
        .map(|((g, _), _, t)| (g.as_str(), *t))
        .collect();
    timings.sort_by(|a, b| b.1.cmp(&a.1));
    for (group, elapsed) in timings.iter().take(5) {
        tracing::debug!("  slowest: {} {:?}", group, elapsed);
    }

    let mut discovered: std::collections::HashMap<(String, String), Vec<ResourceInfo>> =
        std::collections::HashMap::new();

    for ((group_name, version), result, _elapsed) in results {
        let group = match result {
            Ok(g) => g,
            Err(e) => {
                tracing::debug!(group = %group_name, error = %e, "Failed to discover API group");
                continue;
            }
        };

        let mut resources = Vec::new();
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
                custom_fields: None, // Will be populated by fetch_crd_schemas()
            };

            resources.push(info);
        }

        discovered.insert((group_name, version), resources);
    }

    Ok(discovered)
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
            ColumnDef::text("spec", "spec"),
            ColumnDef::text("status", "status"),
        ]),

        // ==================== RBAC: rules pattern ====================
        // Role and ClusterRole have rules array, not spec/status
        "roles" | "clusterroles" => Some(vec![
            ColumnDef::text("rules", "rules"),
            ColumnDef::text("aggregation_rule", "aggregationRule"),
        ]),

        // ==================== RBAC: binding pattern ====================
        // RoleBinding and ClusterRoleBinding reference a role and subjects
        "rolebindings" | "clusterrolebindings" => Some(vec![
            ColumnDef::text("role_ref", "roleRef"),
            ColumnDef::text("subjects", "subjects"),
        ]),

        // ==================== ServiceAccount: flat fields ====================
        "serviceaccounts" => Some(vec![
            ColumnDef::text("secrets", "secrets"),
            ColumnDef::text("image_pull_secrets", "imagePullSecrets"),
            ColumnDef::text(
                "automount_service_account_token",
                "automountServiceAccountToken",
            ),
        ]),

        // ==================== Endpoints: subsets pattern ====================
        "endpoints" => Some(vec![ColumnDef::text("subsets", "subsets")]),

        // ==================== ConfigMap/Secret: data pattern ====================
        "configmaps" => Some(vec![
            ColumnDef::text("data", "data"),
            ColumnDef::text("binary_data", "binaryData"),
            ColumnDef::text("immutable", "immutable"),
        ]),
        "secrets" => Some(vec![
            ColumnDef::text("type", "type"),
            ColumnDef::text("data", "data"),
            ColumnDef::text("string_data", "stringData"),
            ColumnDef::text("immutable", "immutable"),
        ]),

        // ==================== Events: flat structure ====================
        "events" => Some(vec![
            ColumnDef::text("type", "type"),
            ColumnDef::text("reason", "reason"),
            ColumnDef::text("message", "message"),
            ColumnDef::integer("count", "count"),
            ColumnDef::timestamp("first_timestamp", "firstTimestamp"),
            ColumnDef::timestamp("last_timestamp", "lastTimestamp"),
            ColumnDef::text("involved_object", "involvedObject"),
            ColumnDef::text("source", "source"),
        ]),

        // ==================== Metrics: special structure ====================
        "podmetrics" => Some(vec![
            ColumnDef::timestamp("timestamp", "timestamp"),
            ColumnDef::text("window", "window"),
            ColumnDef::text("containers", "containers"),
        ]),
        "nodemetrics" => Some(vec![
            ColumnDef::timestamp("timestamp", "timestamp"),
            ColumnDef::text("window", "window"),
            ColumnDef::text("usage", "usage"),
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
        ColumnDef::text("api_version", "apiVersion"),
        ColumnDef::text("kind", "kind"),
        // Common metadata columns
        ColumnDef::text("name", "metadata.name"),
        ColumnDef::text("namespace", "metadata.namespace"),
        ColumnDef::text("uid", "metadata.uid"),
        ColumnDef::timestamp("created", "metadata.creationTimestamp"),
        ColumnDef::text("labels", "metadata.labels"),
        ColumnDef::text("annotations", "metadata.annotations"),
        ColumnDef::text("owner_references", "metadata.ownerReferences"),
        ColumnDef::integer("generation", "metadata.generation"),
        ColumnDef::text("resource_version", "metadata.resourceVersion"),
        ColumnDef::timestamp("deletion_timestamp", "metadata.deletionTimestamp"),
        ColumnDef::text("finalizers", "metadata.finalizers"),
    ];

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
        columns.push(ColumnDef::text("spec", "spec"));
        columns.push(ColumnDef::text("status", "status"));
    }

    columns
}
