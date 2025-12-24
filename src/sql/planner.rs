// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Query planner for optimizing Kubernetes API calls.
//!
//! The planner analyzes SQL predicates and determines:
//! - Which clusters to query (avoid querying all if not needed)
//! - Which namespaces to query (push down to API)
//! - Label selectors to push to API
//! - Field selectors to push to API (limited support)
//! - Remaining filters to apply client-side

use super::ast::{Condition, Operator, SelectQuery, Value};

/// Specifies which clusters to query
#[derive(Debug, Clone)]
pub enum ClusterScope {
    /// Query only the current context (default)
    Current,
    /// Query specific clusters by name
    Specific(Vec<String>),
    /// Query all known clusters
    All,
}

/// Specifies which namespaces to query within a cluster
#[derive(Debug, Clone)]
pub enum NamespaceScope {
    /// Query all namespaces (Api::all)
    All,
    /// Query a specific namespace (Api::namespaced)
    Specific(String),
}

/// Parameters to push down to the Kubernetes API
#[derive(Debug, Clone, Default)]
pub struct ApiFilters {
    /// Label selector string (e.g., "app=nginx,version=v1")
    pub label_selector: Option<String>,
    /// Field selector string (e.g., "status.phase=Running")
    pub field_selector: Option<String>,
}

/// Conditions that must be evaluated client-side after fetching
#[derive(Debug, Clone)]
pub struct ClientFilters {
    pub conditions: Vec<Condition>,
}

impl Default for ClientFilters {
    fn default() -> Self {
        Self { conditions: Vec::new() }
    }
}

/// The execution plan for a SELECT query
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// Which clusters to query
    pub cluster_scope: ClusterScope,
    /// Which namespaces to query (per-cluster, but usually uniform)
    pub namespace_scope: NamespaceScope,
    /// Filters to push to Kubernetes API
    pub api_filters: ApiFilters,
    /// Filters to apply client-side
    pub client_filters: ClientFilters,
    /// The table (resource type) being queried
    pub table: String,
}

/// Query planner that analyzes SQL and produces optimized execution plans
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn new() -> Self {
        Self
    }

    /// Plan a SELECT query, extracting optimizable predicates
    pub fn plan(&self, query: &SelectQuery) -> QueryPlan {
        let mut cluster_scope = ClusterScope::Current;
        let mut namespace_scope = NamespaceScope::All;
        let mut label_conditions: Vec<(String, String)> = Vec::new();
        let mut field_conditions: Vec<(String, String)> = Vec::new();
        let mut client_conditions: Vec<Condition> = Vec::new();

        // If table_ref has explicit cluster, use that
        if let Some(ref cluster) = query.table_ref.cluster {
            cluster_scope = ClusterScope::Specific(vec![cluster.clone()]);
        }

        // Analyze WHERE clause
        if let Some(ref wc) = query.where_clause {
            for cond in &wc.conditions {
                match self.classify_condition(cond) {
                    ConditionClass::Cluster(scope) => {
                        // Only override if not already set by table_ref
                        if query.table_ref.cluster.is_none() {
                            cluster_scope = scope;
                        }
                    }
                    ConditionClass::Namespace(ns) => {
                        namespace_scope = NamespaceScope::Specific(ns);
                    }
                    ConditionClass::Label(key, value) => {
                        label_conditions.push((key, value));
                    }
                    ConditionClass::Field(field, value) => {
                        field_conditions.push((field, value));
                    }
                    ConditionClass::ClientSide(c) => {
                        client_conditions.push(c);
                    }
                }
            }
        }

        // Build label selector string
        let label_selector = if label_conditions.is_empty() {
            None
        } else {
            Some(
                label_conditions
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        // Build field selector string
        let field_selector = if field_conditions.is_empty() {
            None
        } else {
            Some(
                field_conditions
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(","),
            )
        };

        QueryPlan {
            cluster_scope,
            namespace_scope,
            api_filters: ApiFilters {
                label_selector,
                field_selector,
            },
            client_filters: ClientFilters {
                conditions: client_conditions,
            },
            table: query.table_ref.table.clone(),
        }
    }

    /// Classify a condition to determine how it should be handled
    fn classify_condition(&self, cond: &Condition) -> ConditionClass {
        match cond.column.as_str() {
            // _cluster conditions determine which clusters to query
            "_cluster" => self.classify_cluster_condition(cond),

            // namespace with exact match can be pushed to API
            "namespace" => self.classify_namespace_condition(cond),

            // Labels can be pushed to K8s API as label selectors
            col if col.starts_with("labels.") || col.starts_with("metadata.labels.") => {
                self.classify_label_condition(cond)
            }

            // Certain fields can use field selectors (very limited in K8s)
            "status.phase" => self.classify_field_condition(cond, "status.phase"),
            "spec.nodeName" => self.classify_field_condition(cond, "spec.nodeName"),
            "metadata.name" => self.classify_field_condition(cond, "metadata.name"),

            // Everything else is client-side
            _ => ConditionClass::ClientSide(cond.clone()),
        }
    }

    fn classify_cluster_condition(&self, cond: &Condition) -> ConditionClass {
        match (&cond.operator, &cond.value) {
            (Operator::Eq, Value::String(s)) => {
                if s == "*" {
                    ConditionClass::Cluster(ClusterScope::All)
                } else {
                    ConditionClass::Cluster(ClusterScope::Specific(vec![s.clone()]))
                }
            }
            (Operator::In, Value::List(values)) => {
                let clusters: Vec<String> = values
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if clusters.is_empty() {
                    ConditionClass::Cluster(ClusterScope::Current)
                } else {
                    ConditionClass::Cluster(ClusterScope::Specific(clusters))
                }
            }
            // Non-pushable _cluster conditions (LIKE, etc.) - query all, filter client-side
            _ => {
                ConditionClass::Cluster(ClusterScope::All)
            }
        }
    }

    fn classify_namespace_condition(&self, cond: &Condition) -> ConditionClass {
        match (&cond.operator, &cond.value) {
            // Exact match - push to API
            (Operator::Eq, Value::String(ns)) => ConditionClass::Namespace(ns.clone()),
            // IN with single value - push to API
            (Operator::In, Value::List(values)) if values.len() == 1 => {
                if let Some(Value::String(ns)) = values.first() {
                    ConditionClass::Namespace(ns.clone())
                } else {
                    ConditionClass::ClientSide(cond.clone())
                }
            }
            // Everything else (LIKE, IN with multiple, etc.) - client-side
            _ => ConditionClass::ClientSide(cond.clone()),
        }
    }

    fn classify_label_condition(&self, cond: &Condition) -> ConditionClass {
        // Extract label key from column name
        let label_key = cond
            .column
            .strip_prefix("labels.")
            .or_else(|| cond.column.strip_prefix("metadata.labels."))
            .unwrap_or(&cond.column);

        match (&cond.operator, &cond.value) {
            // Exact match - can push to K8s label selector
            (Operator::Eq, Value::String(val)) => {
                ConditionClass::Label(label_key.to_string(), val.clone())
            }
            // Everything else - client-side
            _ => ConditionClass::ClientSide(cond.clone()),
        }
    }

    fn classify_field_condition(&self, cond: &Condition, field: &str) -> ConditionClass {
        match (&cond.operator, &cond.value) {
            // Exact match - can potentially use field selector
            (Operator::Eq, Value::String(val)) => {
                ConditionClass::Field(field.to_string(), val.clone())
            }
            // Everything else - client-side
            _ => ConditionClass::ClientSide(cond.clone()),
        }
    }
}

/// Classification of how a condition should be handled
enum ConditionClass {
    /// Determines cluster scope
    Cluster(ClusterScope),
    /// Push namespace to API
    Namespace(String),
    /// Push label selector to API
    Label(String, String),
    /// Push field selector to API
    Field(String, String),
    /// Must evaluate client-side
    ClientSide(Condition),
}

impl std::fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "QueryPlan {{")?;
        writeln!(f, "  table: {}", self.table)?;
        writeln!(f, "  clusters: {:?}", self.cluster_scope)?;
        writeln!(f, "  namespace: {:?}", self.namespace_scope)?;
        if let Some(ref ls) = self.api_filters.label_selector {
            writeln!(f, "  label_selector: {}", ls)?;
        }
        if let Some(ref fs) = self.api_filters.field_selector {
            writeln!(f, "  field_selector: {}", fs)?;
        }
        if !self.client_filters.conditions.is_empty() {
            writeln!(f, "  client_filters: {} conditions", self.client_filters.conditions.len())?;
        }
        write!(f, "}}")
    }
}
