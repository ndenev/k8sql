// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion TableProvider implementation for Kubernetes resources

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use tracing::{debug, info};

use crate::kubernetes::ApiFilters;
use crate::kubernetes::K8sClientPool;
use crate::kubernetes::discovery::{ColumnDef, ResourceInfo, generate_schema};

use super::convert::to_arrow_schema;
use super::execution::{K8sExecutionPlan, QueryTarget};
use super::filter_extraction::{ExpressionVisitor, FilterExtractor, FilterValue, walk_expressions};

/// Represents how clusters should be selected for a query
#[derive(Debug, Clone)]
enum ClusterFilter {
    /// Use the current/default context(s)
    Default,
    /// Query all available clusters
    All,
    /// Query specific clusters (from _cluster = 'x' or _cluster IN (...))
    Include(Vec<String>),
    /// Query all clusters except these (from _cluster NOT IN (...))
    Exclude(Vec<String>),
}

/// Represents how namespaces should be selected for a query
#[derive(Debug, Clone)]
enum NamespaceFilter {
    /// No namespace filter - query all namespaces (cluster-wide)
    All,
    /// Single namespace (namespace = 'x')
    Single(String),
    /// Multiple namespaces (namespace IN ('x', 'y')) - parallel queries
    Multiple(Vec<String>),
}

// FilterValue trait implementations for generic filter extraction

impl FilterValue for ClusterFilter {
    fn default() -> Self {
        ClusterFilter::Default
    }

    fn from_single(value: String) -> Self {
        ClusterFilter::Include(vec![value])
    }

    fn from_multiple(values: Vec<String>) -> Self {
        ClusterFilter::Include(values)
    }

    fn handle_special(value: &str) -> Option<Self> {
        if value == "*" {
            Some(ClusterFilter::All)
        } else {
            None
        }
    }

    fn from_negated(values: Vec<String>) -> Option<Self> {
        Some(ClusterFilter::Exclude(values))
    }
}

impl FilterValue for NamespaceFilter {
    fn default() -> Self {
        NamespaceFilter::All
    }

    fn from_single(value: String) -> Self {
        NamespaceFilter::Single(value)
    }

    fn from_multiple(values: Vec<String>) -> Self {
        NamespaceFilter::Multiple(values)
    }
}

/// Visitor that extracts label selectors from expressions
struct LabelSelectorVisitor<'a> {
    provider: &'a K8sTableProvider,
    selectors: Vec<String>,
}

impl<'a> LabelSelectorVisitor<'a> {
    fn new(provider: &'a K8sTableProvider) -> Self {
        Self {
            provider,
            selectors: Vec::new(),
        }
    }

    fn into_selectors(self) -> Vec<String> {
        self.selectors
    }
}

impl<'a> ExpressionVisitor for LabelSelectorVisitor<'a> {
    fn visit_eq(&mut self, binary: &datafusion::logical_expr::BinaryExpr) {
        if let Some(selector) = self.provider.extract_label_selector(binary, "=") {
            self.selectors.push(selector);
        }
    }

    fn visit_not_eq(&mut self, binary: &datafusion::logical_expr::BinaryExpr) {
        if let Some(selector) = self.provider.extract_label_selector(binary, "!=") {
            self.selectors.push(selector);
        }
    }

    fn visit_in_list(&mut self, in_list: &datafusion::logical_expr::expr::InList) {
        if let Some(selector) = self.provider.extract_label_selector_from_inlist(in_list) {
            self.selectors.push(selector);
        }
    }
}

/// Visitor that extracts field selectors from expressions
struct FieldSelectorVisitor<'a> {
    provider: &'a K8sTableProvider,
    registry: &'a crate::kubernetes::FieldSelectorRegistry,
    selectors: Vec<crate::kubernetes::FieldSelector>,
}

impl<'a> FieldSelectorVisitor<'a> {
    fn new(
        provider: &'a K8sTableProvider,
        registry: &'a crate::kubernetes::FieldSelectorRegistry,
    ) -> Self {
        Self {
            provider,
            registry,
            selectors: Vec::new(),
        }
    }

    fn into_selectors(self) -> Vec<crate::kubernetes::FieldSelector> {
        self.selectors
    }
}

impl<'a> ExpressionVisitor for FieldSelectorVisitor<'a> {
    fn visit_eq(&mut self, binary: &datafusion::logical_expr::BinaryExpr) {
        use crate::kubernetes::FieldSelectorOperator;
        if let Some(selector) = self.provider.extract_field_selector(
            binary,
            FieldSelectorOperator::Equals,
            self.registry,
        ) {
            self.selectors.push(selector);
        }
    }

    fn visit_not_eq(&mut self, binary: &datafusion::logical_expr::BinaryExpr) {
        use crate::kubernetes::FieldSelectorOperator;
        if let Some(selector) = self.provider.extract_field_selector(
            binary,
            FieldSelectorOperator::NotEquals,
            self.registry,
        ) {
            self.selectors.push(selector);
        }
    }

    fn visit_in_list(&mut self, _in_list: &datafusion::logical_expr::expr::InList) {
        // Field selectors don't support IN lists currently
        // K8s API only supports = and != operators for field selectors
    }
}

/// A DataFusion TableProvider that fetches data from Kubernetes
pub struct K8sTableProvider {
    /// The resource info for this table
    resource_info: ResourceInfo,
    /// Shared K8s client pool
    pool: Arc<K8sClientPool>,
    /// Arrow schema for this table
    schema: SchemaRef,
    /// Cached column definitions (avoids regenerating for every batch)
    columns: Arc<Vec<ColumnDef>>,
}

impl K8sTableProvider {
    pub fn new(resource_info: ResourceInfo, pool: Arc<K8sClientPool>) -> Self {
        let columns = generate_schema(&resource_info);
        let schema = to_arrow_schema(&columns);

        Self {
            resource_info,
            pool,
            schema,
            columns: Arc::new(columns),
        }
    }

    /// Extract namespace filter from DataFusion expressions
    /// Supports: namespace = 'x', namespace IN (...), and OR chains
    fn extract_namespace_filter(&self, filters: &[Expr]) -> NamespaceFilter {
        FilterExtractor::new("namespace").extract(filters)
    }

    /// Extract _cluster filter from DataFusion expressions
    /// Supports: _cluster = 'x', _cluster = '*', _cluster IN (...), _cluster NOT IN (...)
    fn extract_cluster_filter(&self, filters: &[Expr]) -> ClusterFilter {
        FilterExtractor::new("_cluster").extract(filters)
    }

    /// Extract label selectors from DataFusion expressions
    /// Looks for patterns like `json_get_str(labels, 'key') = 'value'`
    /// Returns a K8s label selector string like "app=nginx,env=prod"
    fn extract_label_selectors(&self, filters: &[Expr]) -> Option<String> {
        let mut visitor = LabelSelectorVisitor::new(self);

        for filter in filters {
            walk_expressions(filter, &mut visitor);
        }

        let selectors = visitor.into_selectors();
        if selectors.is_empty() {
            None
        } else {
            Some(selectors.join(","))
        }
    }

    /// Helper to extract label key from json_get_str(labels, 'key') or labels->>'key' expression
    /// Returns Some(key) if the expression matches the label accessor pattern
    fn extract_label_key_from_json_accessor(&self, expr: &Expr) -> Option<String> {
        if let Expr::ScalarFunction(func) = expr {
            let func_name = func.name();

            if (func_name == "json_get_str" || func_name == "json_as_text")
                && func.args.len() >= 2
                && let Expr::Column(col) = &func.args[0]
                && col.name == "labels"
                && let Expr::Literal(key_lit, _) = &func.args[1]
                && let datafusion::common::ScalarValue::Utf8(Some(key)) = key_lit
            {
                return Some(key.clone());
            }
        }
        None
    }

    /// Extract a label selector from labels->>'key' = 'value' pattern
    /// (labels->>'key' is internally json_as_text; json_get_str is also supported)
    fn extract_label_selector(
        &self,
        binary: &datafusion::logical_expr::BinaryExpr,
        op: &str,
    ) -> Option<String> {
        debug!(
            left = ?binary.left,
            right = ?binary.right,
            "Analyzing binary expression for label selector"
        );

        // Extract label key using helper
        let key = self.extract_label_key_from_json_accessor(binary.left.as_ref())?;

        // Validate right side is a string literal
        if let Expr::Literal(val_lit, _) = binary.right.as_ref()
            && let datafusion::common::ScalarValue::Utf8(Some(value)) = val_lit
        {
            debug!(key = %key, value = %value, "Extracted label selector");
            return Some(format!("{}{}{}", key, op, value));
        }
        None
    }

    /// Extract a label selector from labels->>'key' IN ('val1', 'val2') pattern
    /// Supports both IN and NOT IN for K8s set-based label selectors
    ///
    /// Reference: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#set-based-requirement
    fn extract_label_selector_from_inlist(
        &self,
        in_list: &datafusion::logical_expr::expr::InList,
    ) -> Option<String> {
        debug!(
            expr = ?in_list.expr,
            list_len = in_list.list.len(),
            negated = in_list.negated,
            "Analyzing InList expression for label selector"
        );

        // Extract label key using helper
        let key = self.extract_label_key_from_json_accessor(in_list.expr.as_ref())?;

        // Extract string literal values from the list
        let mut values = Vec::new();
        for item in &in_list.list {
            if let Expr::Literal(val_lit, _) = item
                && let datafusion::common::ScalarValue::Utf8(Some(value)) = val_lit
            {
                values.push(value.clone());
            } else {
                // Non-literal in list, cannot push down
                debug!("Non-literal value in IN list, cannot push down");
                return None;
            }
        }

        // Empty list not supported by K8s
        if values.is_empty() {
            debug!("Empty IN list, cannot push down");
            return None;
        }

        // Build K8s set-based label selector
        let operator = if in_list.negated { "notin" } else { "in" };
        let selector = format!("{} {} ({})", key, operator, values.join(","));
        debug!(selector = %selector, "Extracted set-based label selector");
        Some(selector)
    }

    /// Extract field selectors from DataFusion expressions
    /// Looks for patterns like `status->>'phase' = 'Running'` or `name = 'pod-123'`
    /// Returns a K8s field selector string like "status.phase=Running,metadata.name=pod-123"
    fn extract_field_selectors(&self, filters: &[Expr]) -> Option<String> {
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        let mut visitor = FieldSelectorVisitor::new(self, &FIELD_SELECTOR_REGISTRY);

        for filter in filters {
            walk_expressions(filter, &mut visitor);
        }

        let mut selectors = visitor.into_selectors();
        if selectors.is_empty() {
            None
        } else {
            // Deduplicate selectors to avoid redundant API parameters
            // Sort first to group identical selectors together
            selectors.sort_by(|a, b| {
                a.path
                    .cmp(&b.path)
                    .then(a.operator.cmp(&b.operator))
                    .then(a.value.cmp(&b.value))
            });
            selectors.dedup();

            // Join multiple selectors with commas
            Some(
                selectors
                    .iter()
                    .map(|s| s.to_k8s_string())
                    .collect::<Vec<_>>()
                    .join(","),
            )
        }
    }

    /// Extract a field selector from spec/status/metadata field patterns
    /// Handles three patterns:
    /// 1. json_get_str(spec/status, 'key') = 'value' -> spec.key=value
    /// 2. json_as_text(spec/status, 'key') = 'value' -> spec.key=value (from ->> operator)
    /// 3. name = 'pod-123' -> metadata.name=pod-123 (top-level column)
    fn extract_field_selector(
        &self,
        binary: &datafusion::logical_expr::BinaryExpr,
        operator: crate::kubernetes::FieldSelectorOperator,
        registry: &crate::kubernetes::FieldSelectorRegistry,
    ) -> Option<crate::kubernetes::FieldSelector> {
        use crate::kubernetes::FieldSelector;
        use datafusion::common::ScalarValue;

        debug!(
            left = ?binary.left,
            right = ?binary.right,
            "Analyzing binary expression for field selector"
        );

        // Pattern 1 & 2: json_get_str(spec/status, 'key') = 'value' or spec->>'key' = 'value'
        // Note: ->> is internally represented as json_as_text by datafusion-functions-json
        if let Expr::ScalarFunction(func) = binary.left.as_ref() {
            let func_name = func.name();
            debug!(
                func_name = %func_name,
                args_len = func.args.len(),
                "Found ScalarFunction on left side"
            );

            if (func_name == "json_get_str" || func_name == "json_as_text")
                && func.args.len() >= 2
                && let Expr::Column(col) = &func.args[0]
                && let Expr::Literal(key_lit, _) = &func.args[1]
                && let ScalarValue::Utf8(Some(key)) = key_lit
                && let Expr::Literal(val_lit, _) = binary.right.as_ref()
                && let ScalarValue::Utf8(Some(value)) = val_lit
            {
                // Build field path: "spec.nodeName", "status.phase", etc.
                let field_path = format!("{}.{}", col.name, key);

                // Note: Values come from DataFusion's validated SQL literals, not raw user input.
                // kube-rs handles URL encoding when building API requests.

                // Check if this field selector is supported for this resource type
                if registry.is_supported(&self.resource_info.table_name, &field_path) {
                    debug!(
                        field_path = %field_path,
                        value = %value,
                        "Extracted field selector via json accessor"
                    );
                    return Some(FieldSelector {
                        path: field_path,
                        operator,
                        value: value.clone(),
                    });
                } else {
                    debug!(
                        field_path = %field_path,
                        table = %self.resource_info.table_name,
                        "Field selector not supported for this resource type"
                    );
                }
            }
        }

        // Pattern 3: Top-level column = 'value' (e.g., name = 'pod-123' -> metadata.name=pod-123)
        // Some resources also have top-level columns that map to field selectors
        // (e.g., 'type' for secrets, 'reason' for events)
        if let Expr::Column(col) = binary.left.as_ref()
            && let Expr::Literal(val_lit, _) = binary.right.as_ref()
            && let ScalarValue::Utf8(Some(value)) = val_lit
        {
            // Special case: 'name' column maps to 'metadata.name'
            let field_path = if col.name == "name" {
                "metadata.name".to_string()
            } else {
                col.name.clone()
            };

            // Note: Values come from DataFusion's validated SQL literals, not raw user input.
            // kube-rs handles URL encoding when building API requests.

            // Check if this field selector is supported for this resource type
            if registry.is_supported(&self.resource_info.table_name, &field_path) {
                debug!(
                    column = %col.name,
                    field_path = %field_path,
                    value = %value,
                    "Extracted field selector from top-level column"
                );
                return Some(FieldSelector {
                    path: field_path,
                    operator,
                    value: value.clone(),
                });
            }
        }

        None
    }
}

impl std::fmt::Debug for K8sTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sTableProvider")
            .field("table", &self.resource_info.table_name)
            .finish()
    }
}

/// Returns the appropriate filter pushdown level for equality operators.
///
/// `!=` (NotEq) needs Inexact to ensure SQL NULL semantics:
/// - K8s treats `key!=value` as "not equal OR missing"
/// - SQL treats `NULL != 'value'` as NULL (excluded from results)
///
/// By returning Inexact, DataFusion re-applies the filter after K8s API.
fn pushdown_for_equality_op(op: Operator) -> TableProviderFilterPushDown {
    if op == Operator::NotEq {
        TableProviderFilterPushDown::Inexact
    } else {
        TableProviderFilterPushDown::Exact
    }
}

#[async_trait]
impl TableProvider for K8sTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        // We can push down these filters to the K8s API:
        // - namespace = 'x' -> Uses namespaced API
        // - namespace IN (...) -> Parallel queries to each namespace
        // - _cluster = 'x' -> Queries specific cluster
        // - _cluster IN (...) / NOT IN (...) -> Queries specific clusters
        // - labels->>'key' = 'value' -> K8s label selector
        // - status->>'phase' = 'Running' -> K8s field selector (resource-specific)
        // - name = 'pod-123' -> K8s field selector (metadata.name)
        // Other filters will be handled by DataFusion

        Ok(filters
            .iter()
            .map(|f| {
                // Check for _cluster IN (...) or NOT IN (...)
                // Check for namespace IN (...) - parallel queries to each namespace
                if let Expr::InList(in_list) = f
                    && let Expr::Column(col) = in_list.expr.as_ref()
                    && (col.name == "_cluster" || (col.name == "namespace" && !in_list.negated))
                {
                    return TableProviderFilterPushDown::Exact;
                }

                // Check for labels->>'key' IN (...) or NOT IN (...)
                // K8s supports set-based label selectors: env in (prod,staging)
                if let Expr::InList(in_list) = f
                    && let Expr::ScalarFunction(func) = in_list.expr.as_ref()
                {
                    let func_name = func.name();
                    if (func_name == "json_get_str" || func_name == "json_as_text")
                        && func.args.len() >= 2
                        && let Expr::Column(col) = &func.args[0]
                        && col.name == "labels"
                        && let Expr::Literal(_, _) = &func.args[1]
                    {
                        // Verify all values in list are string literals
                        let all_literals = in_list.list.iter().all(|item| {
                            matches!(
                                item,
                                Expr::Literal(datafusion::common::ScalarValue::Utf8(_), _)
                            )
                        });

                        if all_literals && !in_list.list.is_empty() {
                            // NOT IN needs Inexact to ensure SQL NULL semantics
                            // (same reasoning as label != operator)
                            if in_list.negated {
                                return TableProviderFilterPushDown::Inexact;
                            }
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                }

                if let Expr::BinaryExpr(binary) = f {
                    // Check for OR chains: (_cluster = 'a') OR (_cluster = 'b')
                    // DataFusion rewrites IN lists to OR chains before calling us
                    if binary.op == Operator::Or {
                        // Try to extract cluster or namespace filter from OR expression
                        let cluster_filter = FilterExtractor::<ClusterFilter>::new("_cluster")
                            .extract(&[(*f).clone()]);
                        let namespace_filter = FilterExtractor::<NamespaceFilter>::new("namespace")
                            .extract(&[(*f).clone()]);

                        if !matches!(cluster_filter, ClusterFilter::Default)
                            || !matches!(namespace_filter, NamespaceFilter::All)
                        {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                    // Check for column-based filters (namespace, _cluster)
                    if let Expr::Column(col) = binary.left.as_ref()
                        && (col.name == "namespace" || col.name == "_cluster")
                    {
                        return TableProviderFilterPushDown::Exact;
                    }
                    // Check for labels->>'key' = 'value' or != 'value' pattern
                    // Note: ->> is internally represented as json_as_text by datafusion-functions-json
                    if let Expr::ScalarFunction(func) = binary.left.as_ref() {
                        let func_name = func.name();
                        if (func_name == "json_get_str" || func_name == "json_as_text")
                            && !func.args.is_empty()
                            && let Expr::Column(col) = &func.args[0]
                            && col.name == "labels"
                            && (binary.op == Operator::Eq || binary.op == Operator::NotEq)
                            && matches!(binary.right.as_ref(), Expr::Literal(..))
                        {
                            return pushdown_for_equality_op(binary.op);
                        }

                        // NEW: Check for field selector patterns
                        // Pattern 1 & 2: json_get_str(spec/status, 'key') = 'value' or spec->>'key' = 'value'
                        if (func_name == "json_get_str" || func_name == "json_as_text")
                            && func.args.len() >= 2
                            && let Expr::Column(col) = &func.args[0]
                            && let Expr::Literal(key_lit, _) = &func.args[1]
                            && let datafusion::common::ScalarValue::Utf8(Some(key)) = key_lit
                            && (binary.op == Operator::Eq || binary.op == Operator::NotEq)
                            && matches!(binary.right.as_ref(), Expr::Literal(..))
                        {
                            // Build field path and check if supported
                            let field_path = format!("{}.{}", col.name, key);
                            if FIELD_SELECTOR_REGISTRY
                                .is_supported(&self.resource_info.table_name, &field_path)
                            {
                                return pushdown_for_equality_op(binary.op);
                            }
                        }
                    }

                    // NEW: Check for top-level column field selectors
                    // Pattern 3: name = 'pod-123' or type = 'Opaque' (for secrets/events)
                    if let Expr::Column(col) = binary.left.as_ref()
                        && (binary.op == Operator::Eq || binary.op == Operator::NotEq)
                        && matches!(binary.right.as_ref(), Expr::Literal(..))
                    {
                        // Special case: 'name' column maps to 'metadata.name'
                        let field_path = if col.name == "name" {
                            "metadata.name"
                        } else {
                            &col.name
                        };

                        if FIELD_SELECTOR_REGISTRY
                            .is_supported(&self.resource_info.table_name, field_path)
                        {
                            return pushdown_for_equality_op(binary.op);
                        }
                    }
                }
                // We don't push this filter to K8s API - DataFusion must apply it
                TableProviderFilterPushDown::Unsupported
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Log raw filters for debugging
        debug!(
            table = %self.resource_info.table_name,
            filter_count = filters.len(),
            filters = ?filters,
            "Raw filters passed to scan"
        );

        // Extract pushdown filters - these go to the K8s API to reduce data fetched
        let namespace_filter = self.extract_namespace_filter(filters);
        let cluster_filter = self.extract_cluster_filter(filters);
        let label_selector = self.extract_label_selectors(filters);
        let field_selector = self.extract_field_selectors(filters);

        // Log cluster and namespace filters for debugging
        debug!(
            table = %self.resource_info.table_name,
            cluster_filter = ?cluster_filter,
            namespace_filter = ?namespace_filter,
            labels = ?label_selector,
            fields = ?field_selector,
            "Extracted filters for K8s API"
        );

        // Build API filters with label and field selectors
        let api_filters = ApiFilters {
            label_selector,
            field_selector,
        };

        // Determine which cluster(s) to query based on filter
        let all_contexts = self.pool.list_contexts().unwrap_or_default();
        let clusters: Vec<String> = match cluster_filter {
            ClusterFilter::Default => {
                // Use current context(s) from pool
                self.pool.current_contexts().await
            }
            ClusterFilter::All => {
                // Query all available clusters
                all_contexts
            }
            ClusterFilter::Include(list) => {
                // Query only specified clusters (validate they exist)
                list.into_iter()
                    .filter(|c| all_contexts.contains(c))
                    .collect()
            }
            ClusterFilter::Exclude(exclude_list) => {
                // Query all clusters except the excluded ones
                all_contexts
                    .into_iter()
                    .filter(|c| !exclude_list.contains(c))
                    .collect()
            }
        };

        // Log resolved clusters for debugging
        debug!(
            table = %self.resource_info.table_name,
            clusters = ?clusters,
            available_contexts = ?self.pool.list_contexts().unwrap_or_default(),
            "Resolved clusters to query"
        );

        // Build query targets: (cluster, Option<namespace>) pairs
        // For namespace IN (...), we create separate targets per (cluster, namespace) pair
        // This enables parallel queries to each namespace across all clusters
        let query_targets: Vec<QueryTarget> = match &namespace_filter {
            NamespaceFilter::All => {
                // Query all namespaces (cluster-wide) for each cluster
                clusters
                    .iter()
                    .map(|c| QueryTarget {
                        cluster: c.clone(),
                        namespace: None,
                    })
                    .collect()
            }
            NamespaceFilter::Single(ns) => {
                // Query specific namespace for each cluster
                clusters
                    .iter()
                    .map(|c| QueryTarget {
                        cluster: c.clone(),
                        namespace: Some(ns.clone()),
                    })
                    .collect()
            }
            NamespaceFilter::Multiple(namespaces) => {
                // Query each (cluster, namespace) pair in parallel
                // This creates C × N targets for C clusters and N namespaces
                clusters
                    .iter()
                    .flat_map(|c| {
                        namespaces.iter().map(move |ns| QueryTarget {
                            cluster: c.clone(),
                            namespace: Some(ns.clone()),
                        })
                    })
                    .collect()
            }
        };

        let num_targets = query_targets.len();
        let num_clusters = clusters.len();

        info!(
            table = %self.resource_info.table_name,
            clusters = num_clusters,
            targets = num_targets,
            namespace_filter = ?namespace_filter,
            labels = ?api_filters.label_selector,
            fields = ?api_filters.field_selector,
            limit = ?limit,
            "Creating lazy K8s execution plan"
        );

        // Report query start for progress tracking
        // Note: actual fetching happens lazily when DataFusion executes partitions
        self.pool
            .progress()
            .start_query(&self.resource_info.table_name, num_targets);

        // Projection pushdown optimization: filter columns before conversion.
        // For queries like "SELECT name FROM pods", we skip parsing large JSON fields.
        //
        // Three cases:
        // 1. Non-empty projection: Convert only requested columns (optimization)
        // 2. Empty projection (COUNT(*)): Convert minimal column, wrap with ProjectionExec
        // 3. No projection (None): Convert all columns
        let (exec_schema, exec_columns) = match projection {
            Some(proj) if !proj.is_empty() => {
                // Non-empty projection: push down column filtering
                let fields: Vec<_> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
                let columns: Vec<_> = proj.iter().map(|&i| self.columns[i].clone()).collect();

                debug!(
                    table = %self.resource_info.table_name,
                    total_columns = self.columns.len(),
                    projected_columns = columns.len(),
                    "Projection pushdown - converting only requested columns"
                );

                (Arc::new(Schema::new(fields)), Arc::new(columns))
            }
            Some(_) => {
                // Empty projection (COUNT(*)): convert only minimal column for efficiency.
                // Use _cluster (column 0) since it's small and always present.
                // ProjectionExec will discard it to produce 0-column output.
                let fields = vec![self.schema.field(0).clone()];
                let columns = vec![self.columns[0].clone()];

                debug!(
                    table = %self.resource_info.table_name,
                    "Empty projection (COUNT(*)) - converting minimal column"
                );

                (Arc::new(Schema::new(fields)), Arc::new(columns))
            }
            None => {
                // No projection: use full schema
                (self.schema.clone(), self.columns.clone())
            }
        };

        // Create lazy execution plan - data will be fetched when partitions are executed.
        // Each partition corresponds to one QueryTarget (cluster, namespace pair).
        // DataFusion controls parallelism via its thread pool.
        //
        // Note: DataFusion only passes `limit` when it's safe to apply at the source.
        // This is determined by supports_filters_pushdown() - we return Unsupported for
        // filters we don't push, so DataFusion adds FilterExec and won't push limit through it.
        let plan = K8sExecutionPlan::new(
            self.resource_info.table_name.clone(),
            query_targets,
            api_filters,
            self.pool.clone(),
            exec_schema,
            limit,
            exec_columns,
        );

        // Handle empty projection (COUNT(*)) with ProjectionExec wrapper.
        // We return the minimal column from K8sExec, and ProjectionExec discards it to produce empty output.
        if let Some(proj) = projection
            && proj.is_empty()
        {
            use datafusion::physical_plan::projection::ProjectionExec;

            debug!(
                table = %self.resource_info.table_name,
                "Empty projection (COUNT(*)) - using ProjectionExec wrapper"
            );

            let input_plan: Arc<dyn ExecutionPlan> = Arc::new(plan);
            // Empty projection - no expressions needed
            let empty_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> =
                vec![];
            let projection_plan = ProjectionExec::try_new(empty_exprs, input_plan)?;
            return Ok(Arc::new(projection_plan));
        }

        Ok(Arc::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::logical_expr::BinaryExpr;
    use datafusion::logical_expr::expr::InList;
    use kube::discovery::{ApiCapabilities, ApiResource, Scope};

    /// Helper to create a K8sTableProvider for testing
    fn create_test_provider() -> K8sTableProvider {
        use crate::kubernetes::discovery::ResourceInfo;
        use crate::progress::create_progress_handle;

        let api_resource = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("", "v1", "Pod"),
            "pods",
        );

        let capabilities = ApiCapabilities {
            scope: Scope::Namespaced,
            subresources: vec![],
            operations: vec![],
        };

        let resource_info = ResourceInfo {
            api_resource,
            capabilities,
            table_name: "pods".to_string(),
            aliases: vec!["pod".to_string()],
            is_core: true,
            group: "".to_string(),
            version: "v1".to_string(),
            custom_fields: None,
        };

        // Create a minimal pool - won't actually connect in tests
        let progress = create_progress_handle();
        let pool = Arc::new(K8sClientPool::new_for_test(progress));

        K8sTableProvider::new(resource_info, pool)
    }

    /// Helper to create a binary filter expression (column op value)
    fn binary_filter(column: &str, op: Operator, value: impl Into<String>) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified(column))),
            op,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some(value.into())),
                None,
            )),
        })
    }

    /// Helper to create equality filter (column = value)
    fn eq_filter(column: &str, value: impl Into<String>) -> Expr {
        binary_filter(column, Operator::Eq, value)
    }

    /// Helper to create not-equals filter (column != value)
    fn ne_filter(column: &str, value: impl Into<String>) -> Expr {
        binary_filter(column, Operator::NotEq, value)
    }

    /// Helper to create greater-than filter (column > value)
    fn gt_filter(column: &str, value: impl Into<String>) -> Expr {
        binary_filter(column, Operator::Gt, value)
    }

    /// Helper to create LIKE filter (column LIKE pattern)
    fn like_filter(column: &str, pattern: impl Into<String>) -> Expr {
        binary_filter(column, Operator::LikeMatch, pattern)
    }

    /// Helper to create IN filter (column IN (values))
    fn in_filter(column: &str, values: &[&str]) -> Expr {
        Expr::InList(InList {
            expr: Box::new(Expr::Column(Column::new_unqualified(column))),
            list: values
                .iter()
                .map(|v| {
                    Expr::Literal(
                        datafusion::common::ScalarValue::Utf8(Some(v.to_string())),
                        None,
                    )
                })
                .collect(),
            negated: false,
        })
    }

    /// Helper to assert Exact pushdown
    fn assert_exact(result: &[TableProviderFilterPushDown]) {
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    /// Helper to assert Inexact pushdown
    fn assert_inexact(result: &[TableProviderFilterPushDown]) {
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Inexact));
    }

    /// Helper to assert Unsupported pushdown
    fn assert_unsupported(result: &[TableProviderFilterPushDown]) {
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0],
            TableProviderFilterPushDown::Unsupported
        ));
    }

    #[test]
    fn test_filter_pushdown_namespace_equals() {
        let provider = create_test_provider();
        let filter = eq_filter("namespace", "default");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_exact(&result);
    }

    #[test]
    fn test_filter_pushdown_cluster_equals() {
        let provider = create_test_provider();
        let filter = eq_filter("_cluster", "prod");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_exact(&result);
    }

    #[test]
    fn test_filter_pushdown_namespace_in_list() {
        let provider = create_test_provider();
        let filter = in_filter("namespace", &["ns1", "ns2"]);
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_exact(&result);
    }

    #[test]
    fn test_filter_pushdown_name_field_selector() {
        let provider = create_test_provider();
        let filter = eq_filter("name", "my-pod");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_exact(&result);
    }

    #[test]
    fn test_filter_pushdown_unsupported_status_field() {
        let provider = create_test_provider();
        // status = 'Running' is not pushed to K8s API
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("status"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("Running".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0],
            TableProviderFilterPushDown::Unsupported
        ));
    }

    // Note: JSON accessor field selector tests (status->>'phase', spec->>'nodeName')
    // are tested via integration/manual testing since constructing ScalarFunction
    // expressions manually is complex. The field selector extraction logic is covered
    // by the registry tests and top-level column tests below.

    #[test]
    fn test_field_selector_top_level_secret_type() {
        use crate::kubernetes::discovery::ResourceInfo;
        use crate::progress::create_progress_handle;

        // Create test provider for secrets (which support 'type' field selector)
        let api_resource = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("", "v1", "Secret"),
            "secrets",
        );

        let capabilities = ApiCapabilities {
            scope: Scope::Namespaced,
            subresources: vec![],
            operations: vec![],
        };

        let resource_info = ResourceInfo {
            api_resource,
            capabilities,
            table_name: "secrets".to_string(),
            aliases: vec!["secret".to_string()],
            is_core: true,
            group: "".to_string(),
            version: "v1".to_string(),
            custom_fields: None,
        };

        let progress = create_progress_handle();
        let pool = Arc::new(K8sClientPool::new_for_test(progress));
        let provider = K8sTableProvider::new(resource_info, pool);

        // type = 'Opaque' (top-level column for secrets)
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("type"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("Opaque".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_field_selector_static_registry_initialization() {
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        // Access registry multiple times to ensure it's initialized once
        let first_access = FIELD_SELECTOR_REGISTRY.is_supported("pods", "status.phase");
        let second_access = FIELD_SELECTOR_REGISTRY.is_supported("pods", "status.phase");

        assert!(first_access);
        assert!(second_access);

        // Verify registry contains expected resources
        assert!(FIELD_SELECTOR_REGISTRY.is_supported("pods", "metadata.name"));
        assert!(FIELD_SELECTOR_REGISTRY.is_supported("secrets", "type"));
        assert!(FIELD_SELECTOR_REGISTRY.is_supported("events", "reason"));
    }

    #[test]
    fn test_field_selector_multiple_conditions_and() {
        let provider = create_test_provider();

        // Test AND with two field selectors: name = 'pod-1' AND status = 'Running'
        // Note: We can't test status->>'phase' easily without ScalarFunction construction,
        // but we can test top-level columns
        let name_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-1".to_string())),
                None,
            )),
        });

        // Create a combined AND expression
        let combined_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(name_filter.clone()),
            op: Operator::And,
            right: Box::new(Expr::Column(Column::new_unqualified("namespace"))),
        });

        let result = provider
            .supports_filters_pushdown(&[&combined_filter])
            .unwrap();

        // The AND expression should be analyzed and both sides should be pushable
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_field_selector_name_metadata_mapping() {
        let provider = create_test_provider();

        // Verify that 'name' column is correctly mapped to 'metadata.name'
        let name_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("my-pod".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&name_filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_field_selector_name_not_equals_inexact() {
        let provider = create_test_provider();
        let filter = ne_filter("name", "excluded-pod");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_inexact(&result);
    }

    #[test]
    fn test_field_selector_unsupported_operator_greater_than() {
        let provider = create_test_provider();
        let filter = gt_filter("name", "pod-a");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_unsupported(&result);
    }

    #[test]
    fn test_field_selector_unsupported_operator_like() {
        let provider = create_test_provider();
        let filter = like_filter("name", "pod-%");
        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();
        assert_unsupported(&result);
    }

    #[test]
    fn test_field_selector_no_state_leakage_across_queries() {
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        // Simulate multiple queries to ensure no state leakage
        let provider1 = create_test_provider();
        let provider2 = create_test_provider();

        // First query
        let filter1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-1".to_string())),
                None,
            )),
        });

        let result1 = provider1.supports_filters_pushdown(&[&filter1]).unwrap();
        assert!(matches!(result1[0], TableProviderFilterPushDown::Exact));

        // Second query - registry should still work correctly
        let filter2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-2".to_string())),
                None,
            )),
        });

        let result2 = provider2.supports_filters_pushdown(&[&filter2]).unwrap();
        assert!(matches!(result2[0], TableProviderFilterPushDown::Exact));

        // Verify registry is still intact
        assert!(FIELD_SELECTOR_REGISTRY.is_supported("pods", "status.phase"));
        assert!(FIELD_SELECTOR_REGISTRY.is_supported("secrets", "type"));
    }

    #[test]
    fn test_field_selector_extraction_format() {
        let provider = create_test_provider();
        let name_filter = eq_filter("name", "my-pod");
        let result = provider.extract_field_selectors(&[name_filter]);

        // Should produce "metadata.name=my-pod"
        assert!(result.is_some());
        let field_selector_string = result.unwrap();
        assert_eq!(field_selector_string, "metadata.name=my-pod");
    }

    #[test]
    fn test_field_selector_resource_specific_secret_type() {
        use crate::kubernetes::discovery::ResourceInfo;
        use crate::progress::create_progress_handle;

        // Create provider for secrets to test resource-specific field
        let api_resource = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("", "v1", "Secret"),
            "secrets",
        );

        let capabilities = ApiCapabilities {
            scope: Scope::Namespaced,
            subresources: vec![],
            operations: vec![],
        };

        let resource_info = ResourceInfo {
            api_resource,
            capabilities,
            table_name: "secrets".to_string(),
            aliases: vec!["secret".to_string()],
            is_core: true,
            group: "".to_string(),
            version: "v1".to_string(),
            custom_fields: None,
        };

        let progress = create_progress_handle();
        let pool = Arc::new(K8sClientPool::new_for_test(progress));
        let provider = K8sTableProvider::new(resource_info, pool);

        // Test type field for secrets
        let type_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("type"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("Opaque".to_string())),
                None,
            )),
        });

        // Extract field selectors
        let result = provider.extract_field_selectors(&[type_filter]);

        // Should produce "type=Opaque"
        assert!(result.is_some());
        let field_selector_string = result.unwrap();
        assert_eq!(field_selector_string, "type=Opaque");
    }

    #[test]
    fn test_field_selector_scan_integration() {
        use crate::kubernetes::ApiFilters;

        let provider = create_test_provider();

        // Create a filter that should extract field selector
        let name_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("test-pod".to_string())),
                None,
            )),
        });

        // Test extraction
        let field_selector = provider.extract_field_selectors(&[name_filter]);

        assert!(field_selector.is_some());
        assert_eq!(field_selector.as_ref().unwrap(), "metadata.name=test-pod");

        // Verify it can be used in ApiFilters
        let api_filters = ApiFilters {
            label_selector: None,
            field_selector,
        };

        assert!(api_filters.field_selector.is_some());
        assert_eq!(
            api_filters.field_selector.unwrap(),
            "metadata.name=test-pod"
        );
    }

    #[test]
    fn test_field_selector_multiple_combined() {
        // For secrets, create a type filter
        use crate::kubernetes::discovery::ResourceInfo;
        use crate::progress::create_progress_handle;

        let api_resource = ApiResource::from_gvk_with_plural(
            &kube::api::GroupVersionKind::gvk("", "v1", "Secret"),
            "secrets",
        );

        let capabilities = ApiCapabilities {
            scope: Scope::Namespaced,
            subresources: vec![],
            operations: vec![],
        };

        let resource_info = ResourceInfo {
            api_resource,
            capabilities,
            table_name: "secrets".to_string(),
            aliases: vec!["secret".to_string()],
            is_core: true,
            group: "".to_string(),
            version: "v1".to_string(),
            custom_fields: None,
        };

        let progress = create_progress_handle();
        let pool = Arc::new(K8sClientPool::new_for_test(progress));
        let secret_provider = K8sTableProvider::new(resource_info, pool);

        let name_filter_secret = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("my-secret".to_string())),
                None,
            )),
        });

        let type_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("type"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("Opaque".to_string())),
                None,
            )),
        });

        // Extract both field selectors
        let combined = secret_provider.extract_field_selectors(&[name_filter_secret, type_filter]);

        assert!(combined.is_some());
        let combined_str = combined.unwrap();

        // Should contain both selectors, comma-separated
        // Order may vary, so check both are present
        assert!(combined_str.contains("metadata.name=my-secret"));
        assert!(combined_str.contains("type=Opaque"));
        assert!(combined_str.contains(","));
    }

    #[test]
    fn test_field_selector_with_namespace_filter() {
        let provider = create_test_provider();

        // Combine namespace filter (uses scoped API) with field selector
        let namespace_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("namespace"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("default".to_string())),
                None,
            )),
        });

        let name_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("my-pod".to_string())),
                None,
            )),
        });

        // Test pushdown - both should be Exact
        let result = provider
            .supports_filters_pushdown(&[&namespace_filter, &name_filter])
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
        assert!(matches!(result[1], TableProviderFilterPushDown::Exact));

        // Test extraction - namespace handled separately, field selector extracted
        let namespace_extracted = provider.extract_namespace_filter(&[namespace_filter]);
        let field_selector = provider.extract_field_selectors(&[name_filter]);

        // Namespace should be Single("default")
        assert!(matches!(
            namespace_extracted,
            crate::datafusion_integration::provider::NamespaceFilter::Single(_)
        ));

        // Field selector should be metadata.name=my-pod
        assert!(field_selector.is_some());
        assert_eq!(field_selector.unwrap(), "metadata.name=my-pod");
    }

    #[test]
    fn test_field_selector_deduplication() {
        let provider = create_test_provider();

        // Create duplicate filters: name = 'pod-1' AND name = 'pod-1'
        let name_filter1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-1".to_string())),
                None,
            )),
        });

        let name_filter2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-1".to_string())),
                None,
            )),
        });

        // Extract should deduplicate
        let field_selector = provider.extract_field_selectors(&[name_filter1, name_filter2]);

        assert!(field_selector.is_some());
        // Should only contain one instance, not "metadata.name=pod-1,metadata.name=pod-1"
        assert_eq!(field_selector.unwrap(), "metadata.name=pod-1");
    }

    #[test]
    fn test_filter_pushdown_mixed_filters() {
        let provider = create_test_provider();

        // namespace = 'default' (pushable)
        let namespace_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("namespace"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("default".to_string())),
                None,
            )),
        });

        // name = 'test' (now pushable as metadata.name field selector)
        let name_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("test".to_string())),
                None,
            )),
        });

        let result = provider
            .supports_filters_pushdown(&[&namespace_filter, &name_filter])
            .unwrap();

        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
        assert!(matches!(result[1], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_field_selector_integer_string_value() {
        // Test that field selectors work with integer values as strings
        // Example: status.replicas = "3" (K8s API accepts string representation)
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        assert!(FIELD_SELECTOR_REGISTRY.is_supported("replicasets", "status.replicas"));

        // Verify the field selector format would be correct with string value
        use crate::kubernetes::{FieldSelector, FieldSelectorOperator};
        let selector = FieldSelector {
            path: "status.replicas".to_string(),
            operator: FieldSelectorOperator::Equals,
            value: "3".to_string(), // Integer as string
        };

        assert_eq!(selector.to_k8s_string(), "status.replicas=3");

        // This confirms that when we extract status->>'replicas' = '3' in the future,
        // it will work correctly with K8s API (which accepts "3" as a string)
    }

    #[test]
    fn test_field_selector_boolean_string_value() {
        // Test that field selectors work with boolean values as strings
        // Example: spec.unschedulable = "true" (K8s API accepts string representation)
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        assert!(FIELD_SELECTOR_REGISTRY.is_supported("nodes", "spec.unschedulable"));

        // Verify the field selector format works with boolean string values
        use crate::kubernetes::{FieldSelector, FieldSelectorOperator};
        let selector_true = FieldSelector {
            path: "spec.unschedulable".to_string(),
            operator: FieldSelectorOperator::Equals,
            value: "true".to_string(), // Boolean as string
        };

        let selector_false = FieldSelector {
            path: "spec.unschedulable".to_string(),
            operator: FieldSelectorOperator::Equals,
            value: "false".to_string(), // Boolean as string
        };

        assert_eq!(selector_true.to_k8s_string(), "spec.unschedulable=true");
        assert_eq!(selector_false.to_k8s_string(), "spec.unschedulable=false");

        // This confirms that when we extract spec->>'unschedulable' = 'true' in the future,
        // it will work correctly with K8s API (which accepts "true"/"false" as strings)
    }

    #[test]
    fn test_field_selector_or_expression_unsupported() {
        // Test that OR expressions are unsupported for field selectors
        // K8s field selectors only support AND (comma-separated), not OR
        let provider = create_test_provider();

        let name_filter1 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-1".to_string())),
                None,
            )),
        });

        let name_filter2 = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("pod-2".to_string())),
                None,
            )),
        });

        // Create OR expression: name = 'pod-1' OR name = 'pod-2'
        let or_filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(name_filter1),
            op: Operator::Or,
            right: Box::new(name_filter2),
        });

        // OR should be Unsupported (K8s doesn't support OR in field selectors)
        let result = provider.supports_filters_pushdown(&[&or_filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(
            result[0],
            TableProviderFilterPushDown::Unsupported
        ));

        // Verify extraction doesn't produce anything for OR
        let extracted = provider.extract_field_selectors(&[or_filter]);
        assert!(extracted.is_none());
    }

    #[tokio::test]
    async fn test_projection_pushdown() {
        use datafusion::prelude::SessionContext;

        let provider = create_test_provider();
        let ctx = SessionContext::new();

        // Test 1: Non-empty projection should push down
        // Request only columns 0 (_cluster) and 3 (name)
        let projection = Some(vec![0, 3]);

        let plan = provider
            .scan(&ctx.state(), projection.as_ref(), &[], None)
            .await
            .unwrap();

        // The schema should contain only the projected columns
        let schema = plan.schema();
        assert_eq!(
            schema.fields().len(),
            2,
            "Projected schema should have 2 fields"
        );
        assert_eq!(schema.field(0).name(), "_cluster");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_projection_pushdown_empty() {
        use datafusion::prelude::SessionContext;

        let provider = create_test_provider();
        let ctx = SessionContext::new();

        // Test 2: Empty projection (COUNT(*)) should use ProjectionExec wrapper
        let projection = Some(vec![]);

        let plan = provider
            .scan(&ctx.state(), projection.as_ref(), &[], None)
            .await
            .unwrap();

        // For empty projection, we wrap with ProjectionExec
        // The schema should be empty (0 columns)
        let schema = plan.schema();
        assert_eq!(
            schema.fields().len(),
            0,
            "Empty projection should have 0 fields in schema"
        );

        // Verify it's a ProjectionExec
        assert_eq!(plan.name(), "ProjectionExec");
    }

    #[tokio::test]
    async fn test_projection_pushdown_none() {
        use datafusion::prelude::SessionContext;

        let provider = create_test_provider();
        let ctx = SessionContext::new();

        // Test 3: No projection (None) should use full schema
        let projection: Option<&Vec<usize>> = None;

        let plan = provider
            .scan(&ctx.state(), projection, &[], None)
            .await
            .unwrap();

        // The schema should contain all columns (16 for pods)
        let schema = plan.schema();
        assert_eq!(
            schema.fields().len(),
            16,
            "Full schema should have all 16 fields"
        );

        // Verify it's our K8sExec plan (not wrapped)
        assert_eq!(plan.name(), "K8sExec");
    }
}
