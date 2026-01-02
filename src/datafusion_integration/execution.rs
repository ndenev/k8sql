// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Custom ExecutionPlan for lazy Kubernetes resource fetching
//!
//! This module provides a DataFusion ExecutionPlan that fetches data lazily,
//! allowing DataFusion to control parallelism and enabling streaming execution.
//! Results are streamed as pages arrive from the K8s API for optimal memory usage.
//!
//! # LIMIT Pushdown
//!
//! LIMIT pushdown to the Kubernetes API is **disabled** due to a DataFusion API limitation.
//! When a query has an OFFSET clause, DataFusion passes a combined `skip + fetch` value to
//! `TableProvider::scan()`, making it impossible to distinguish between:
//! - `LIMIT 10` (scan receives limit=10, should fetch 10 rows)
//! - `LIMIT 10 OFFSET 5` (scan receives limit=15, breaking OFFSET semantics)
//!
//! Example bug scenario:
//! ```sql
//! SELECT * FROM pods ORDER BY name LIMIT 10 OFFSET 5
//! ```
//! With pushdown enabled, we fetch 15 rows from K8s API, then DataFusion applies OFFSET.
//! But if the ORDER BY requires sorting across all data, we've already limited the K8s
//! fetch and might skip rows that should appear in the final result.
//!
//! All LIMIT/OFFSET handling is performed by DataFusion's LimitExec to ensure correctness.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use async_stream::try_stream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use tracing::debug;

use crate::kubernetes::discovery::ColumnDef;
use crate::kubernetes::{ApiFilters, K8sClientPool};

use super::convert::json_to_record_batch;

/// Check if an error is a Kubernetes "not found" (404) error
///
/// Returns true for 404 errors which indicate the namespace or resource
/// doesn't exist. These are expected in cross-cluster queries where not
/// all clusters have the same namespaces.
fn is_not_found_error(err: &anyhow::Error) -> bool {
    if let Some(kube::Error::Api(api_err)) = err.downcast_ref::<kube::Error>() {
        return api_err.code == 404;
    }
    false
}

/// Check if an error is an "unknown table" error
///
/// Returns true for errors indicating the table/resource doesn't exist in
/// a cluster's registry. This is expected in wildcard queries where different
/// clusters may have different CRDs.
///
/// Note: This uses string matching as a pragmatic approach since the error
/// originates from anyhow!() in client.rs. For a more robust solution,
/// consider defining a custom K8sError enum in a follow-up PR.
fn is_unknown_table_error(err: &anyhow::Error) -> bool {
    let err_msg = err.to_string();
    // Match the specific error message format from client.rs:812
    err_msg.starts_with("Unknown table:") || err_msg.contains("Unknown table: '")
}

/// A query target representing a specific (cluster, namespace) pair to fetch
#[derive(Debug, Clone)]
pub struct QueryTarget {
    /// The cluster context name
    pub cluster: String,
    /// Optional namespace (None = cluster-wide query)
    pub namespace: Option<String>,
}

/// Custom ExecutionPlan that lazily fetches Kubernetes resources
///
/// Each partition corresponds to one QueryTarget (cluster, namespace pair).
/// Data is fetched only when DataFusion requests it via the execute method.
/// Results are streamed as pages arrive from the K8s API.
pub struct K8sExecutionPlan {
    /// Table name for logging and progress reporting
    table_name: String,
    /// Query targets - one per partition
    targets: Vec<QueryTarget>,
    /// API filters (labels, field selectors)
    api_filters: ApiFilters,
    /// Client pool for API access
    pool: Arc<K8sClientPool>,
    /// Arrow schema for the output
    schema: SchemaRef,
    /// Cached plan properties
    plan_properties: PlanProperties,
    /// Optional row limit for early termination
    fetch_limit: Option<usize>,
    /// Execution metrics for EXPLAIN ANALYZE
    metrics: ExecutionPlanMetricsSet,
    /// Cached column definitions (passed to conversion, avoids regenerating)
    columns: Arc<Vec<ColumnDef>>,
    /// Wall-clock timer for tracking actual query duration across all partitions
    /// Starts when first partition executes, tracks overall time
    /// Uses OnceLock for zero-overhead reads after initialization (no locking)
    wall_clock_start: Arc<OnceLock<Instant>>,
}

impl K8sExecutionPlan {
    /// Create a new K8sExecutionPlan
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_name: String,
        targets: Vec<QueryTarget>,
        api_filters: ApiFilters,
        pool: Arc<K8sClientPool>,
        schema: SchemaRef,
        fetch_limit: Option<usize>,
        columns: Arc<Vec<ColumnDef>>,
    ) -> Self {
        // Compute plan properties
        let partitioning = Partitioning::UnknownPartitioning(targets.len().max(1));
        let equivalence_properties = EquivalenceProperties::new(schema.clone());
        let plan_properties = PlanProperties::new(
            equivalence_properties,
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            targets,
            api_filters,
            pool,
            schema,
            plan_properties,
            fetch_limit,
            metrics: ExecutionPlanMetricsSet::new(),
            columns,
            wall_clock_start: Arc::new(OnceLock::new()),
        }
    }
}

impl fmt::Debug for K8sExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("K8sExecutionPlan")
            .field("table", &self.table_name)
            .field("partitions", &self.targets.len())
            .field("fetch_limit", &self.fetch_limit)
            .finish()
    }
}

impl DisplayAs for K8sExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                // Concise: just table name and partition count
                write!(
                    f,
                    "K8sExec: table={}, partitions={}",
                    self.table_name,
                    self.targets.len()
                )?;
                if let Some(limit) = self.fetch_limit {
                    write!(f, ", fetch={}", limit)?;
                }
                Ok(())
            }
            DisplayFormatType::Verbose | DisplayFormatType::TreeRender => {
                // Verbose: show per-partition cluster mapping
                writeln!(
                    f,
                    "K8sExec: table={}, partitions={}",
                    self.table_name,
                    self.targets.len()
                )?;
                if let Some(limit) = self.fetch_limit {
                    writeln!(f, "  fetch={}", limit)?;
                }

                // Show which cluster each partition targets
                for (i, target) in self.targets.iter().enumerate() {
                    write!(f, "  partition[{}]: cluster={}", i, target.cluster)?;
                    if let Some(ref ns) = target.namespace {
                        write!(f, ", namespace={}", ns)?;
                    }
                    // Don't add newline after last partition - DataFusion appends metrics on same line
                    if i < self.targets.len() - 1 {
                        writeln!(f)?;
                    }
                }

                Ok(())
            }
        }
    }
}

impl ExecutionPlan for K8sExecutionPlan {
    fn name(&self) -> &str {
        "K8sExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node - no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Leaf node - should not have new children
        if children.is_empty() {
            Ok(self)
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "K8sExecutionPlan has no children".to_string(),
            ))
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        // Add wall-clock time if query has started
        // OnceLock provides zero-overhead reads (no locking after initialization)
        if let Some(start) = self.wall_clock_start.get() {
            let elapsed_ms = start.elapsed().as_millis() as usize;
            // wall_clock_time (gauge, ms): Actual query duration from first partition start
            // This differs from total_fetch_time (which sums parallel work) and represents
            // the user-visible query execution time for parallel multi-cluster queries.
            let wall_clock = MetricBuilder::new(&self.metrics).gauge("wall_clock_time", 0);
            wall_clock.set(elapsed_ms);
        }

        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        // LIMIT pushdown is disabled due to DataFusion passing skip+fetch combined
        // to TableProvider::scan() when OFFSET is present (see module docs)
        false
    }

    fn with_fetch(&self, _fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        // LIMIT pushdown disabled - see module-level documentation
        None
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch_limit
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Handle empty targets case - return empty stream
        if self.targets.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::empty(),
            )));
        }

        // Validate partition index
        if partition >= self.targets.len() {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Partition {} out of range (max {})",
                partition,
                self.targets.len()
            )));
        }

        // Start wall-clock timer on first partition execution
        // OnceLock::get_or_init ensures only the first partition sets the timer
        self.wall_clock_start.get_or_init(Instant::now);

        // Get the target for this partition
        let target = self.targets[partition].clone();
        let table_name = self.table_name.clone();
        let api_filters = self.api_filters.clone();
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let fetch_limit = self.fetch_limit;
        let columns = self.columns.clone();

        // Set up metrics for this partition
        // Counters (aggregate via sum):
        let rows_fetched = MetricBuilder::new(&self.metrics).counter("rows_fetched", partition); // count: K8s resources returned
        let pages_fetched = MetricBuilder::new(&self.metrics).counter("pages_fetched", partition); // count: K8s API calls (LIST operations)
        let output_bytes = MetricBuilder::new(&self.metrics).counter("output_bytes", partition); // bytes: Arrow RecordBatch memory size

        // Time metrics in milliseconds (aggregate via sum):
        let fetch_time =
            MetricBuilder::new(&self.metrics).subset_time("total_fetch_time", partition); // ms: total K8s API fetch time (summed across partitions)
        let conversion_time =
            MetricBuilder::new(&self.metrics).subset_time("conversion_time", partition); // ms: JSON to Arrow conversion time
        let first_page_time =
            MetricBuilder::new(&self.metrics).subset_time("first_page_time", partition); // ms: time to first page (TTFB)

        debug!(
            table = %table_name,
            partition = partition,
            cluster = %target.cluster,
            namespace = ?target.namespace,
            fetch_limit = ?fetch_limit,
            "Executing K8s partition"
        );

        // Create a streaming execution that yields RecordBatches as pages arrive
        let stream = create_streaming_execution(
            pool,
            table_name,
            target,
            api_filters,
            fetch_limit,
            columns,
            rows_fetched,
            pages_fetched,
            fetch_time,
            output_bytes,
            conversion_time,
            first_page_time,
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Create a stream that fetches K8s pages and yields RecordBatches
#[allow(clippy::too_many_arguments)]
fn create_streaming_execution(
    pool: Arc<K8sClientPool>,
    table_name: String,
    target: QueryTarget,
    api_filters: ApiFilters,
    fetch_limit: Option<usize>,
    columns: Arc<Vec<ColumnDef>>,
    rows_fetched: datafusion::physical_plan::metrics::Count,
    pages_fetched: datafusion::physical_plan::metrics::Count,
    fetch_time: datafusion::physical_plan::metrics::Time,
    output_bytes: datafusion::physical_plan::metrics::Count,
    conversion_time: datafusion::physical_plan::metrics::Time,
    first_page_time: datafusion::physical_plan::metrics::Time,
) -> Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>> {
    let stream = try_stream! {
        let start = Instant::now();

        // Stream pages from K8s API
        let mut page_stream = pool.stream_resources(
            &table_name,
            target.namespace.as_deref(),
            Some(&target.cluster),
            &api_filters,
            fetch_limit,
        );

        use futures::StreamExt;
        let mut total_rows = 0usize;
        let mut first_page_received = false;
        let first_page_timer = Instant::now();

        while let Some(page_result) = page_stream.next().await {
            match page_result {
                Ok(items) => {
                    // Track time to first page
                    if !first_page_received {
                        first_page_time.add_duration(first_page_timer.elapsed());
                        first_page_received = true;
                    }

                    let page_row_count = items.len();
                    total_rows += page_row_count;

                    // Update metrics
                    pages_fetched.add(1);
                    rows_fetched.add(page_row_count);

                    debug!(
                        cluster = %target.cluster,
                        namespace = ?target.namespace,
                        table = %table_name,
                        page_rows = page_row_count,
                        total_rows = total_rows,
                        "Streaming page from K8s"
                    );

                    // Convert page to RecordBatch and yield
                    if !items.is_empty() {
                        let convert_start = Instant::now();
                        let batch = json_to_record_batch(&target.cluster, &columns, items)?;

                        // Track conversion time and output bytes
                        conversion_time.add_duration(convert_start.elapsed());
                        let batch_bytes = batch.get_array_memory_size();
                        output_bytes.add(batch_bytes);

                        yield batch;
                    }
                }
                Err(e) => {
                    // Check if this is a "not found" error (404 or unknown table)
                    if is_not_found_error(&e) || is_unknown_table_error(&e) {
                        debug!(
                            cluster = %target.cluster,
                            namespace = ?target.namespace,
                            table = %table_name,
                            "Resource/table not found in cluster, returning empty results"
                        );
                        // Don't yield anything, just stop - partition returns no results
                        return;
                    } else {
                        // Real errors should fail
                        Err(datafusion::error::DataFusionError::External(
                            format!(
                                "Failed to fetch {} from cluster '{}'{}: {}",
                                table_name,
                                target.cluster,
                                target.namespace
                                    .as_ref()
                                    .map(|ns| format!(" namespace '{}'", ns))
                                    .unwrap_or_default(),
                                e
                            ).into(),
                        ))?;
                    }
                }
            }
        }

        // Record total fetch time
        fetch_time.add_duration(start.elapsed());

        // Report progress
        pool.progress().cluster_complete(
            &target.cluster,
            total_rows,
            start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        );

        debug!(
            cluster = %target.cluster,
            namespace = ?target.namespace,
            table = %table_name,
            total_rows = total_rows,
            elapsed_ms = start.elapsed().as_millis(),
            "Completed streaming from target"
        );
    };

    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_target_creation() {
        let target = QueryTarget {
            cluster: "test-cluster".to_string(),
            namespace: Some("default".to_string()),
        };

        assert_eq!(target.cluster, "test-cluster");
        assert_eq!(target.namespace, Some("default".to_string()));
    }

    #[test]
    fn test_query_target_cluster_wide() {
        let target = QueryTarget {
            cluster: "test-cluster".to_string(),
            namespace: None,
        };

        assert_eq!(target.cluster, "test-cluster");
        assert!(target.namespace.is_none());
    }

    #[test]
    fn test_is_not_found_error_with_404() {
        let api_err = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "namespaces \"missing\" not found".to_string(),
            reason: "NotFound".to_string(),
            code: 404,
        });
        let err = anyhow::Error::new(api_err);

        assert!(is_not_found_error(&err));
    }

    #[test]
    fn test_is_not_found_error_with_other_code() {
        let api_err = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "Forbidden".to_string(),
            reason: "Forbidden".to_string(),
            code: 403,
        });
        let err = anyhow::Error::new(api_err);

        assert!(!is_not_found_error(&err));
    }

    #[test]
    fn test_is_not_found_error_with_non_api_error() {
        let err = anyhow::anyhow!("Some other error");

        assert!(!is_not_found_error(&err));
    }
}
