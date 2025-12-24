// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion TableProvider implementation for Kubernetes resources

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use tokio::sync::RwLock;

use crate::kubernetes::discovery::{generate_schema, ResourceInfo};
use crate::kubernetes::K8sClientPool;
use crate::sql::ApiFilters;

use super::convert::{json_to_record_batch, to_arrow_schema};

/// A DataFusion TableProvider that fetches data from Kubernetes
pub struct K8sTableProvider {
    /// The resource info for this table
    resource_info: ResourceInfo,
    /// Shared K8s client pool
    pool: Arc<K8sClientPool>,
    /// Arrow schema for this table
    schema: SchemaRef,
    /// Current cluster context (None = use pool's current)
    cluster: Arc<RwLock<Option<String>>>,
}

impl K8sTableProvider {
    pub fn new(resource_info: ResourceInfo, pool: Arc<K8sClientPool>) -> Self {
        let columns = generate_schema(&resource_info);
        let schema = to_arrow_schema(&columns);

        Self {
            resource_info,
            pool,
            schema,
            cluster: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the cluster context for queries
    #[allow(dead_code)]
    pub async fn set_cluster(&self, cluster: Option<String>) {
        *self.cluster.write().await = cluster;
    }

    /// Extract namespace filter from DataFusion expressions if possible
    fn extract_namespace_filter(&self, filters: &[Expr]) -> Option<String> {
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter {
                if let (Expr::Column(col), Expr::Literal(lit, _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "namespace"
                        && matches!(binary.op, datafusion::logical_expr::Operator::Eq)
                    {
                        if let datafusion::common::ScalarValue::Utf8(Some(ns)) = lit {
                            return Some(ns.clone());
                        }
                    }
                }
            }
        }
        None
    }

    /// Extract _cluster filter from DataFusion expressions
    fn extract_cluster_filter(&self, filters: &[Expr]) -> Option<String> {
        for filter in filters {
            if let Expr::BinaryExpr(binary) = filter {
                if let (Expr::Column(col), Expr::Literal(lit, _)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                {
                    if col.name == "_cluster"
                        && matches!(binary.op, datafusion::logical_expr::Operator::Eq)
                    {
                        if let datafusion::common::ScalarValue::Utf8(Some(cluster)) = lit {
                            return Some(cluster.clone());
                        }
                    }
                }
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
        // We can push down namespace and _cluster filters to the K8s API
        // Other filters will be handled by DataFusion
        Ok(filters
            .iter()
            .map(|f| {
                if let Expr::BinaryExpr(binary) = f {
                    if let Expr::Column(col) = binary.left.as_ref() {
                        if col.name == "namespace" || col.name == "_cluster" {
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

        // Determine which cluster(s) to query
        let clusters: Vec<String> = if let Some(ref cf) = cluster_filter {
            if cf == "*" {
                // Query all clusters
                self.pool.list_contexts().unwrap_or_default()
            } else {
                vec![cf.clone()]
            }
        } else {
            // Use current context
            let current = self.pool.current_context().await.unwrap_or_default();
            vec![current]
        };

        // Fetch data from each cluster
        let mut batches = Vec::new();
        let api_filters = ApiFilters::default(); // TODO: Extract label/field selectors

        for cluster in &clusters {
            let resources = self
                .pool
                .fetch_resources(
                    &self.resource_info.table_name,
                    namespace.as_deref(),
                    Some(cluster),
                    &api_filters,
                )
                .await
                .unwrap_or_default();

            if !resources.is_empty() {
                let batch = json_to_record_batch(cluster, &self.resource_info, resources)?;
                batches.push(batch);
            }
        }

        // If no data, create an empty batch with correct schema
        if batches.is_empty() {
            let empty_batch = datafusion::arrow::array::RecordBatch::new_empty(self.schema.clone());
            batches.push(empty_batch);
        }

        // Create MemorySourceConfig execution plan with our batches (one partition)
        // DataFusion's optimizer will automatically apply remaining filters and limits
        let plan = MemorySourceConfig::try_new_exec(
            &[batches],
            self.schema.clone(),
            projection.cloned(),
        )?;

        Ok(plan)
    }
}
