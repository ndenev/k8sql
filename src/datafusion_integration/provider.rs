// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion TableProvider implementation for Kubernetes resources

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use tracing::{debug, info};

use crate::kubernetes::ApiFilters;
use crate::kubernetes::K8sClientPool;
use crate::kubernetes::discovery::{ResourceInfo, generate_schema};

use super::convert::to_arrow_schema;
use super::execution::{K8sExecutionPlan, QueryTarget};
use super::filter_extraction::{FilterExtractor, FilterValue};

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

/// A DataFusion TableProvider that fetches data from Kubernetes
pub struct K8sTableProvider {
    /// The resource info for this table
    resource_info: ResourceInfo,
    /// Shared K8s client pool
    pool: Arc<K8sClientPool>,
    /// Arrow schema for this table
    schema: SchemaRef,
}

impl K8sTableProvider {
    pub fn new(resource_info: ResourceInfo, pool: Arc<K8sClientPool>) -> Self {
        let columns = generate_schema(&resource_info);
        let schema = to_arrow_schema(&columns);

        Self {
            resource_info,
            pool,
            schema,
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
    /// (after SQL preprocessing converts `labels.key` to this form)
    /// Returns a K8s label selector string like "app=nginx,env=prod"
    fn extract_label_selectors(&self, filters: &[Expr]) -> Option<String> {
        let mut selectors = Vec::new();

        for filter in filters {
            self.collect_label_selectors(filter, &mut selectors);
        }

        if selectors.is_empty() {
            None
        } else {
            Some(selectors.join(","))
        }
    }

    /// Recursively collect label selectors from an expression tree
    fn collect_label_selectors(&self, expr: &Expr, selectors: &mut Vec<String>) {
        match expr {
            // Handle AND expressions - recurse into both sides
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.collect_label_selectors(&binary.left, selectors);
                self.collect_label_selectors(&binary.right, selectors);
            }
            // Handle equality: json_get_str(labels, 'key') = 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                if let Some(selector) = self.extract_label_selector(binary, "=") {
                    selectors.push(selector);
                }
            }
            // Handle inequality: json_get_str(labels, 'key') != 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => {
                if let Some(selector) = self.extract_label_selector(binary, "!=") {
                    selectors.push(selector);
                }
            }
            _ => {}
        }
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

        // Handle json_get_str(labels, 'key') = 'value' or labels->>'key' = 'value' pattern
        // Note: ->> is internally represented as json_as_text by datafusion-functions-json
        if let Expr::ScalarFunction(func) = binary.left.as_ref() {
            let func_name = func.name();
            debug!(func_name = %func_name, args_len = func.args.len(), "Found ScalarFunction on left side");

            if (func_name == "json_get_str" || func_name == "json_as_text")
                && func.args.len() >= 2
                && let Expr::Column(col) = &func.args[0]
                && col.name == "labels"
                && let Expr::Literal(key_lit, _) = &func.args[1]
                && let datafusion::common::ScalarValue::Utf8(Some(key)) = key_lit
                && let Expr::Literal(val_lit, _) = binary.right.as_ref()
                && let datafusion::common::ScalarValue::Utf8(Some(value)) = val_lit
            {
                debug!(key = %key, value = %value, "Extracted label selector via json_get_str");
                return Some(format!("{}{}{}", key, op, value));
            }
        }
        None
    }

    /// Extract field selectors from DataFusion expressions
    /// Looks for patterns like `status->>'phase' = 'Running'` or `name = 'pod-123'`
    /// Returns a K8s field selector string like "status.phase=Running,metadata.name=pod-123"
    fn extract_field_selectors(&self, filters: &[Expr]) -> Option<String> {
        use crate::kubernetes::FIELD_SELECTOR_REGISTRY;

        let mut selectors = Vec::new();

        for filter in filters {
            self.collect_field_selectors(filter, &FIELD_SELECTOR_REGISTRY, &mut selectors);
        }

        if selectors.is_empty() {
            None
        } else {
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

    /// Recursively collect field selectors from an expression tree
    fn collect_field_selectors(
        &self,
        expr: &Expr,
        registry: &crate::kubernetes::FieldSelectorRegistry,
        selectors: &mut Vec<crate::kubernetes::FieldSelector>,
    ) {
        use crate::kubernetes::FieldSelectorOperator;

        match expr {
            // Handle AND expressions - recurse into both sides
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                self.collect_field_selectors(&binary.left, registry, selectors);
                self.collect_field_selectors(&binary.right, registry, selectors);
            }
            // Handle equality: json_get_str(spec/status, 'key') = 'value' or name = 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                if let Some(selector) =
                    self.extract_field_selector(binary, FieldSelectorOperator::Equals, registry)
                {
                    selectors.push(selector);
                }
            }
            // Handle inequality: json_get_str(spec/status, 'key') != 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => {
                if let Some(selector) =
                    self.extract_field_selector(binary, FieldSelectorOperator::NotEquals, registry)
                {
                    selectors.push(selector);
                }
            }
            _ => {}
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
                            // != needs Inexact to ensure SQL NULL semantics:
                            // K8s treats key!=value as "not equal OR missing"
                            // SQL treats NULL != 'value' as NULL (excluded from results)
                            // By returning Inexact, DataFusion re-applies the filter
                            if binary.op == Operator::NotEq {
                                return TableProviderFilterPushDown::Inexact;
                            }
                            return TableProviderFilterPushDown::Exact;
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
                                // != needs Inexact for same reasons as labels
                                if binary.op == Operator::NotEq {
                                    return TableProviderFilterPushDown::Inexact;
                                }
                                return TableProviderFilterPushDown::Exact;
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
                            // != needs Inexact for same reasons as labels
                            if binary.op == Operator::NotEq {
                                return TableProviderFilterPushDown::Inexact;
                            }
                            return TableProviderFilterPushDown::Exact;
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

        // Create lazy execution plan - data will be fetched when partitions are executed
        // Each partition corresponds to one QueryTarget (cluster, namespace pair)
        // DataFusion controls parallelism via its thread pool
        //
        // Note: DataFusion only passes `limit` when it's safe to apply at the source.
        // This is determined by supports_filters_pushdown() - we return Unsupported for
        // filters we don't push, so DataFusion adds FilterExec and won't push limit through it.
        let plan = K8sExecutionPlan::new(
            self.resource_info.table_name.clone(),
            self.resource_info.clone(),
            query_targets,
            api_filters,
            self.pool.clone(),
            self.schema.clone(),
            limit,
        );

        // Handle projection if specified
        if let Some(proj) = projection {
            // Create a ProjectionExec to handle column projection
            use datafusion::physical_plan::projection::ProjectionExec;

            let input_plan: Arc<dyn ExecutionPlan> = Arc::new(plan);
            let projected_exprs: Result<Vec<_>> = proj
                .iter()
                .map(|&i| {
                    let field = self.schema.field(i);
                    let col =
                        datafusion::physical_expr::expressions::col(field.name(), &self.schema)
                            .map_err(|e| {
                                datafusion::error::DataFusionError::Internal(format!(
                                    "Column {} not found in schema: {}",
                                    field.name(),
                                    e
                                ))
                            })?;
                    Ok((col, field.name().to_string()))
                })
                .collect();

            let projection_plan = ProjectionExec::try_new(projected_exprs?, input_plan)?;
            Ok(Arc::new(projection_plan))
        } else {
            Ok(Arc::new(plan))
        }
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

    #[test]
    fn test_filter_pushdown_namespace_equals() {
        let provider = create_test_provider();
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("namespace"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("default".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_filter_pushdown_cluster_equals() {
        let provider = create_test_provider();
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("_cluster"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("prod".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_filter_pushdown_namespace_in_list() {
        let provider = create_test_provider();
        let filter = Expr::InList(InList {
            expr: Box::new(Expr::Column(Column::new_unqualified("namespace"))),
            list: vec![
                Expr::Literal(
                    datafusion::common::ScalarValue::Utf8(Some("ns1".to_string())),
                    None,
                ),
                Expr::Literal(
                    datafusion::common::ScalarValue::Utf8(Some("ns2".to_string())),
                    None,
                ),
            ],
            negated: false,
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
    }

    #[test]
    fn test_filter_pushdown_name_field_selector() {
        let provider = create_test_provider();
        // name = 'test' is now pushed as metadata.name field selector
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("name"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::Utf8(Some("test".to_string())),
                None,
            )),
        });

        let result = provider.supports_filters_pushdown(&[&filter]).unwrap();

        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], TableProviderFilterPushDown::Exact));
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
}
