// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Custom ExecutionPlan for lazy Kubernetes resource fetching
//!
//! This module provides a DataFusion ExecutionPlan that fetches data lazily,
//! allowing DataFusion to control parallelism and enabling streaming execution.

use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream;
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
        }
    }
}

impl fmt::Debug for K8sExecutionPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("K8sExecutionPlan")
            .field("table", &self.table_name)
            .field("partitions", &self.targets.len())
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
                )
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

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Handle empty targets case - return empty stream
        if self.targets.is_empty() {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                stream::empty(),
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

        debug!(
            table = %table_name,
            partition = partition,
            cluster = %target.cluster,
            namespace = ?target.namespace,
            "Executing K8s partition"
        );

        // Create a lazy stream that fetches data when polled
        let stream = stream::once(async move {
            let start = Instant::now();

            // Fetch resources from K8s API
            let resources = match pool
                .fetch_resources(
                    &table_name,
                    target.namespace.as_deref(),
                    Some(&target.cluster),
                    &api_filters,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    // Check if this is a "not found" error (404)
                    // These are expected when a namespace doesn't exist in a cluster
                    if is_not_found_error(&e) {
                        debug!(
                            cluster = %target.cluster,
                            namespace = ?target.namespace,
                            table = %table_name,
                            "Resource/namespace not found, returning empty results"
                        );
                        Vec::new()
                    } else {
                        // Real errors (auth, network, server) should fail the query
                        // User can exclude this cluster with _cluster NOT IN ('...') if needed
                        return Err(datafusion::error::DataFusionError::External(
                            format!(
                                "Failed to fetch {} from cluster '{}'{}: {}",
                                table_name,
                                target.cluster,
                                target
                                    .namespace
                                    .as_ref()
                                    .map(|ns| format!(" namespace '{}'", ns))
                                    .unwrap_or_default(),
                                e
                            )
                            .into(),
                        ));
                    }
                }
            };

            let elapsed = start.elapsed();
            let row_count = resources.len();

            debug!(
                cluster = %target.cluster,
                namespace = ?target.namespace,
                table = %table_name,
                rows = row_count,
                elapsed_ms = elapsed.as_millis(),
                "Fetched from target"
            );

            // Report progress (truncate to u64::MAX if somehow > 584 million years)
            pool.progress().cluster_complete(
                &target.cluster,
                row_count,
                elapsed.as_millis().try_into().unwrap_or(u64::MAX),
            );

            // Convert to RecordBatch
            if resources.is_empty() {
                Ok(RecordBatch::new_empty(schema))
            } else {
                json_to_record_batch(&target.cluster, &resource_info, resources)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
