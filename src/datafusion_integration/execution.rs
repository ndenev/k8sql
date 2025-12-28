// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Custom ExecutionPlan for lazy Kubernetes resource fetching
//!
//! This module provides a DataFusion ExecutionPlan that fetches data lazily,
//! allowing DataFusion to control parallelism and enabling streaming execution.
//! Results are streamed as pages arrive from the K8s API for optimal memory usage.
//!
//! # LIMIT Behavior with Multiple Partitions
//!
//! When querying multiple clusters/namespaces (multiple partitions) with a LIMIT:
//!
//! - Each partition independently applies the limit as an optimization hint
//! - For `SELECT * FROM pods WHERE _cluster = '*' LIMIT 10` across 3 clusters:
//!   - Partition 0 (cluster1): fetches up to 10 rows
//!   - Partition 1 (cluster2): fetches up to 10 rows
//!   - Partition 2 (cluster3): fetches up to 10 rows
//!   - K8sExec returns up to 30 rows total
//!   - DataFusion's LimitExec (above K8sExec) then takes the first 10
//!
//! This is the expected behavior for partitioned scans with limit pushdown.
//! We cannot know which partition will provide the "first" N rows until
//! execution, so each partition optimistically fetches up to N rows.
//! The final LIMIT is always correctly enforced by DataFusion's LimitExec.
//!
//! The benefit: instead of fetching ALL rows from all partitions, we fetch
//! at most `limit * num_partitions` rows, which is still a significant
//! reduction for large result sets.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
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

use crate::kubernetes::discovery::ResourceInfo;
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
    /// Resource metadata for schema generation
    resource_info: ResourceInfo,
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
}

impl K8sExecutionPlan {
    /// Create a new K8sExecutionPlan
    pub fn new(
        table_name: String,
        resource_info: ResourceInfo,
        targets: Vec<QueryTarget>,
        api_filters: ApiFilters,
        pool: Arc<K8sClientPool>,
        schema: SchemaRef,
        fetch_limit: Option<usize>,
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
            resource_info,
            targets,
            api_filters,
            pool,
            schema,
            plan_properties,
            fetch_limit,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Create a clone with a new fetch limit
    ///
    /// Note: Metrics are cloned (shared via internal Arc) so EXPLAIN ANALYZE
    /// captures execution stats from any plan variant the optimizer chooses.
    fn with_new_fetch(&self, fetch: Option<usize>) -> Self {
        Self {
            table_name: self.table_name.clone(),
            resource_info: self.resource_info.clone(),
            targets: self.targets.clone(),
            api_filters: self.api_filters.clone(),
            pool: self.pool.clone(),
            schema: self.schema.clone(),
            plan_properties: self.plan_properties.clone(),
            fetch_limit: fetch,
            metrics: self.metrics.clone(),
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
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
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
        Some(self.metrics.clone_inner())
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(self.with_new_fetch(fetch)))
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

        // Get the target for this partition
        let target = self.targets[partition].clone();
        let table_name = self.table_name.clone();
        let resource_info = self.resource_info.clone();
        let api_filters = self.api_filters.clone();
        let pool = self.pool.clone();
        let schema = self.schema.clone();
        let fetch_limit = self.fetch_limit;

        // Set up metrics for this partition
        let rows_fetched = MetricBuilder::new(&self.metrics).counter("rows_fetched", partition);
        let pages_fetched = MetricBuilder::new(&self.metrics).counter("pages_fetched", partition);
        let fetch_time = MetricBuilder::new(&self.metrics).subset_time("fetch_time", partition);

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
            resource_info,
            api_filters,
            schema.clone(),
            fetch_limit,
            rows_fetched,
            pages_fetched,
            fetch_time,
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
    resource_info: ResourceInfo,
    api_filters: ApiFilters,
    schema: SchemaRef,
    fetch_limit: Option<usize>,
    rows_fetched: datafusion::physical_plan::metrics::Count,
    pages_fetched: datafusion::physical_plan::metrics::Count,
    fetch_time: datafusion::physical_plan::metrics::Time,
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

        while let Some(page_result) = page_stream.next().await {
            match page_result {
                Ok(items) => {
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
                        let batch = json_to_record_batch(&target.cluster, &resource_info, items)?;
                        yield batch;
                    }
                }
                Err(e) => {
                    // Check if this is a "not found" error (404)
                    if is_not_found_error(&e) {
                        debug!(
                            cluster = %target.cluster,
                            namespace = ?target.namespace,
                            table = %table_name,
                            "Resource/namespace not found, returning empty results"
                        );
                        // Yield empty batch and stop
                        yield RecordBatch::new_empty(schema.clone());
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
