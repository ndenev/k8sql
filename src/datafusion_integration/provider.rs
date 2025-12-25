// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion TableProvider implementation for Kubernetes resources

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use futures::stream::{self, StreamExt};
use tracing::{debug, info};

/// Maximum number of clusters to query concurrently
/// Higher values improve performance but increase local resource usage (connections, memory)
const MAX_CONCURRENT_CLUSTERS: usize = 15;

use crate::kubernetes::K8sClientPool;
use crate::kubernetes::discovery::{ResourceInfo, generate_schema};
use crate::sql::ApiFilters;

use super::convert::{json_to_record_batch, to_arrow_schema};

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

    /// Extract namespace filter from DataFusion expressions if possible
    fn extract_namespace_filter(&self, filters: &[Expr]) -> Option<String> {
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter
                && let (Expr::Column(col), Expr::Literal(lit, _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                && col.name == "namespace"
                && matches!(binary.op, datafusion::logical_expr::Operator::Eq)
                && let datafusion::common::ScalarValue::Utf8(Some(ns)) = lit
            {
                return Some(ns.clone());
            }
        }
        None
    }

    /// Extract _cluster filter from DataFusion expressions
    /// Supports: _cluster = 'x', _cluster = '*', _cluster IN (...), _cluster NOT IN (...)
    fn extract_cluster_filter(&self, filters: &[Expr]) -> ClusterFilter {
        for filter in filters {
            // Handle _cluster = 'value' or _cluster = '*'
            if let Expr::BinaryExpr(binary) = filter
                && let Expr::Column(col) = binary.left.as_ref()
                && col.name == "_cluster"
                && matches!(binary.op, Operator::Eq)
                && let Expr::Literal(lit, _) = binary.right.as_ref()
                && let datafusion::common::ScalarValue::Utf8(Some(cluster)) = lit
            {
                if cluster == "*" {
                    return ClusterFilter::All;
                }
                return ClusterFilter::Include(vec![cluster.clone()]);
            }

            // Handle _cluster IN ('a', 'b', 'c')
            if let Expr::InList(in_list) = filter
                && let Expr::Column(col) = in_list.expr.as_ref()
                && col.name == "_cluster"
                && !in_list.negated
            {
                let clusters: Vec<String> = in_list
                    .list
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Literal(lit, _) = e
                            && let datafusion::common::ScalarValue::Utf8(Some(s)) = lit
                        {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !clusters.is_empty() {
                    // Check if '*' is in the list - treat as All
                    if clusters.iter().any(|c| c == "*") {
                        return ClusterFilter::All;
                    }
                    return ClusterFilter::Include(clusters);
                }
            }

            // Handle _cluster NOT IN ('a', 'b')
            if let Expr::InList(in_list) = filter
                && let Expr::Column(col) = in_list.expr.as_ref()
                && col.name == "_cluster"
                && in_list.negated
            {
                let clusters: Vec<String> = in_list
                    .list
                    .iter()
                    .filter_map(|e| {
                        if let Expr::Literal(lit, _) = e
                            && let datafusion::common::ScalarValue::Utf8(Some(s)) = lit
                        {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !clusters.is_empty() {
                    return ClusterFilter::Exclude(clusters);
                }
            }
        }
        ClusterFilter::Default
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
            // Handle equality: labels['key'] = 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                if let Some(selector) = self.extract_label_selector(binary, "=") {
                    selectors.push(selector);
                }
            }
            // Handle inequality: labels['key'] != 'value'
            Expr::BinaryExpr(binary) if binary.op == Operator::NotEq => {
                if let Some(selector) = self.extract_label_selector(binary, "!=") {
                    selectors.push(selector);
                }
            }
            _ => {}
        }
    }

    /// Extract a label selector from labels['key'] = 'value' pattern (native map access)
    /// or json_get_str(labels, 'key') = 'value' pattern (legacy)
    fn extract_label_selector(
        &self,
        binary: &datafusion::logical_expr::BinaryExpr,
        op: &str,
    ) -> Option<String> {
        // Debug: log the expression structure
        debug!(
            left = ?binary.left,
            right = ?binary.right,
            "Analyzing binary expression for label selector"
        );

        // Try native map access: get_field(labels, 'key') pattern
        // DataFusion represents labels['key'] as ScalarFunction { name: "get_field", args: [Column("labels"), Literal("key")] }
        if let Expr::ScalarFunction(func) = binary.left.as_ref() {
            let func_name = func.name();
            debug!(func_name = %func_name, args_len = func.args.len(), "Found ScalarFunction on left side");

            // Handle get_field for native map access (labels['key'])
            if func_name == "get_field"
                && func.args.len() >= 2
                && let Expr::Column(col) = &func.args[0]
                && col.name == "labels"
            {
                // Second arg should be the label key as a literal
                if let Expr::Literal(key_lit, _) = &func.args[1]
                    && let datafusion::common::ScalarValue::Utf8(Some(key)) = key_lit
                {
                    // Right side should be the value literal
                    if let Expr::Literal(val_lit, _) = binary.right.as_ref()
                        && let datafusion::common::ScalarValue::Utf8(Some(value)) = val_lit
                    {
                        debug!(key = %key, value = %value, "Extracted label selector via get_field");
                        return Some(format!("{}{}{}", key, op, value));
                    }
                }
            }

            // Legacy: handle json_get_str for backward compatibility
            if func_name == "json_get_str"
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
        // We can push down these filters to the K8s API:
        // - namespace = 'x' -> Uses namespaced API
        // - _cluster = 'x' -> Queries specific cluster
        // - _cluster IN (...) / NOT IN (...) -> Queries specific clusters
        // - labels['key'] = 'value' -> K8s label selector (native map access via get_field)
        // - json_get_str(labels, 'key') = 'value' -> K8s label selector (legacy)
        // Other filters will be handled by DataFusion
        Ok(filters
            .iter()
            .map(|f| {
                // Check for _cluster IN (...) or NOT IN (...)
                if let Expr::InList(in_list) = f
                    && let Expr::Column(col) = in_list.expr.as_ref()
                    && col.name == "_cluster"
                {
                    return TableProviderFilterPushDown::Exact;
                }
                if let Expr::BinaryExpr(binary) = f {
                    // Check for column-based filters (namespace, _cluster)
                    if let Expr::Column(col) = binary.left.as_ref()
                        && (col.name == "namespace" || col.name == "_cluster")
                    {
                        return TableProviderFilterPushDown::Exact;
                    }
                    // Check for labels['key'] pattern (native map access via get_field or json_get_str)
                    if let Expr::ScalarFunction(func) = binary.left.as_ref() {
                        let func_name = func.name();
                        if (func_name == "get_field" || func_name == "json_get_str")
                            && !func.args.is_empty()
                            && let Expr::Column(col) = &func.args[0]
                            && col.name == "labels"
                        {
                            return TableProviderFilterPushDown::Exact;
                        }
                    }
                }
                TableProviderFilterPushDown::Inexact
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Extract pushdown filters - these go to the K8s API to reduce data fetched
        let namespace = self.extract_namespace_filter(filters);
        let cluster_filter = self.extract_cluster_filter(filters);
        let label_selector = self.extract_label_selectors(filters);

        // Log pushdown filters for debugging
        if namespace.is_some() || label_selector.is_some() {
            debug!(
                table = %self.resource_info.table_name,
                namespace = ?namespace,
                labels = ?label_selector,
                "Pushing down filters to K8s API"
            );
        }

        // Build API filters with label selector
        let api_filters = ApiFilters {
            label_selector,
            field_selector: None, // TODO: Add field selector support for status.phase etc.
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

        // Fetch data from all clusters IN PARALLEL
        // Each cluster becomes a separate partition for DataFusion to process
        let table_name = self.resource_info.table_name.clone();
        let resource_info = self.resource_info.clone();
        let pool = self.pool.clone();

        let num_clusters = clusters.len();
        info!(
            table = %table_name,
            clusters = num_clusters,
            namespace = ?namespace,
            labels = ?api_filters.label_selector,
            "Fetching resources from K8s API"
        );

        let fetch_start = Instant::now();

        // Report query start
        pool.progress().start_query(&table_name, num_clusters);

        // Execute cluster queries with concurrency limit to avoid overwhelming APIs
        let results: Vec<_> = stream::iter(clusters.iter().cloned())
            .map(|cluster| {
                let pool = pool.clone();
                let table_name = table_name.clone();
                let namespace = namespace.clone();
                let api_filters = api_filters.clone();

                async move {
                    let start = Instant::now();
                    let resources = pool
                        .fetch_resources(
                            &table_name,
                            namespace.as_deref(),
                            Some(&cluster),
                            &api_filters,
                        )
                        .await
                        .unwrap_or_default();
                    let elapsed = start.elapsed();
                    let row_count = resources.len();
                    debug!(
                        cluster = %cluster,
                        table = %table_name,
                        rows = row_count,
                        elapsed_ms = elapsed.as_millis(),
                        "Fetched from cluster"
                    );
                    // Report cluster completion
                    pool.progress().cluster_complete(&cluster, row_count, elapsed.as_millis() as u64);
                    (cluster, resources)
                }
            })
            // Limit concurrent requests to avoid overwhelming K8s APIs
            .buffer_unordered(MAX_CONCURRENT_CLUSTERS)
            .collect()
            .await;
        let fetch_elapsed = fetch_start.elapsed();

        // Convert results to partitions - one partition per cluster
        // This allows DataFusion to process clusters in parallel
        let mut partitions: Vec<Vec<datafusion::arrow::array::RecordBatch>> = Vec::new();
        let mut total_rows = 0usize;
        let mut batch_schema: Option<SchemaRef> = None;

        for (cluster, resources) in results {
            let row_count = resources.len();
            total_rows += row_count;
            if !resources.is_empty() {
                let batch = json_to_record_batch(&cluster, &resource_info, resources)?;
                // Capture the schema from the first non-empty batch
                if batch_schema.is_none() {
                    batch_schema = Some(batch.schema());
                }
                // Each cluster's data is its own partition
                partitions.push(vec![batch]);
            }
        }

        // Use the inferred schema from batches, or fall back to provider schema for empty results
        let exec_schema = batch_schema.unwrap_or_else(|| self.schema.clone());

        // If no data, create a single empty partition with the schema
        if partitions.is_empty() {
            let empty_batch = datafusion::arrow::array::RecordBatch::new_empty(exec_schema.clone());
            partitions.push(vec![empty_batch]);
        }

        // Report query completion
        pool.progress().query_complete(total_rows, fetch_elapsed.as_millis() as u64);

        info!(
            table = %self.resource_info.table_name,
            clusters = num_clusters,
            partitions = partitions.len(),
            total_rows = total_rows,
            fetch_ms = fetch_elapsed.as_millis(),
            "K8s API fetch complete"
        );

        // Create MemorySourceConfig execution plan with multiple partitions
        // DataFusion's executor will process partitions in parallel
        // Use the batch schema which may have native struct types for spec/status
        let plan = MemorySourceConfig::try_new_exec(&partitions, exec_schema, projection.cloned())?;

        Ok(plan)
    }
}
