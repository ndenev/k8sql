// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion SessionContext setup for k8sql

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::kubernetes::K8sClientPool;
use crate::output::QueryResult;

use super::preprocess::preprocess_sql;
use super::provider::K8sTableProvider;

/// A wrapper around DataFusion's SessionContext configured for K8s queries
pub struct K8sSessionContext {
    ctx: SessionContext,
    pool: Arc<K8sClientPool>,
}

impl K8sSessionContext {
    /// Create a new K8sSessionContext with all discovered K8s resources registered as tables
    pub async fn new(pool: Arc<K8sClientPool>) -> anyhow::Result<Self> {
        // Get the current Kubernetes context name for the catalog
        let cluster_name = pool.current_context().await.unwrap_or_else(|_| "k8s".to_string());

        // Enable information_schema and use cluster name as catalog
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(&cluster_name, "default");
        let mut ctx = SessionContext::new_with_config(config);

        // Register JSON functions for querying spec/status fields
        datafusion_functions_json::register_all(&mut ctx)?;

        // Get the resource registry and register each resource as a table
        let registry = pool.get_registry(None).await?;

        for info in registry.list_tables() {
            let provider = K8sTableProvider::new(info.clone(), Arc::clone(&pool));

            // Register with the primary table name
            ctx.register_table(&info.table_name, Arc::new(provider))?;

            // Also register aliases
            for alias in &info.aliases {
                let provider = K8sTableProvider::new(info.clone(), Arc::clone(&pool));
                // Ignore errors for aliases (might conflict)
                let _ = ctx.register_table(alias, Arc::new(provider));
            }
        }

        Ok(Self { ctx, pool })
    }

    /// Execute a SQL query and return the results as Arrow RecordBatches
    pub async fn execute_sql(&self, sql: &str) -> DFResult<Vec<RecordBatch>> {
        // Preprocess SQL to handle k8sql-specific syntax (e.g., labels.app = 'value')
        let processed_sql = preprocess_sql(sql);
        let df = self.ctx.sql(&processed_sql).await?;
        df.collect().await
    }

    /// Execute a SQL query and return results as formatted strings (for display)
    pub async fn execute_sql_to_strings(&self, sql: &str) -> anyhow::Result<QueryResult> {
        let batches = self.execute_sql(sql).await?;

        if batches.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        // Get column names from schema
        let schema = batches[0].schema();
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

        // Convert batches to rows of strings
        let mut rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = array_value_to_string(col, row_idx);
                    row.push(value);
                }
                rows.push(row);
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// Get pretty-printed output (useful for debugging)
    #[allow(dead_code)]
    pub async fn execute_sql_pretty(&self, sql: &str) -> anyhow::Result<String> {
        let batches = self.execute_sql(sql).await?;
        Ok(pretty_format_batches(&batches)?.to_string())
    }

    /// Refresh the registered tables (call after context switch)
    pub async fn refresh_tables(&mut self) -> anyhow::Result<()> {
        // Get the current Kubernetes context name for the catalog
        let cluster_name = self
            .pool
            .current_context()
            .await
            .unwrap_or_else(|_| "k8s".to_string());

        // Create a new context and re-register all tables
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(&cluster_name, "default");
        let mut new_ctx = SessionContext::new_with_config(config);

        // Register JSON functions
        datafusion_functions_json::register_all(&mut new_ctx)?;

        let registry = self.pool.get_registry(None).await?;

        for info in registry.list_tables() {
            let provider = K8sTableProvider::new(info.clone(), Arc::clone(&self.pool));
            new_ctx.register_table(&info.table_name, Arc::new(provider))?;

            for alias in &info.aliases {
                let provider = K8sTableProvider::new(info.clone(), Arc::clone(&self.pool));
                let _ = new_ctx.register_table(alias, Arc::new(provider));
            }
        }

        self.ctx = new_ctx;
        Ok(())
    }

    /// Get the underlying K8s client pool
    #[allow(dead_code)]
    pub fn pool(&self) -> &Arc<K8sClientPool> {
        &self.pool
    }

    /// Consume this wrapper and return the underlying DataFusion SessionContext
    pub fn into_session_context(self) -> SessionContext {
        self.ctx
    }

    /// List available tables
    #[allow(dead_code)]
    pub fn list_tables(&self) -> Vec<String> {
        self.ctx
            .catalog_names()
            .into_iter()
            .flat_map(|catalog| {
                self.ctx
                    .catalog(&catalog)
                    .map(|c| {
                        c.schema_names()
                            .into_iter()
                            .flat_map(|schema| {
                                c.schema(&schema)
                                    .map(|s| s.table_names())
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect()
    }
}

/// Convert an Arrow array value at a given index to a string
fn array_value_to_string(array: &datafusion::arrow::array::ArrayRef, idx: usize) -> String {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    if array.is_null(idx) {
        return String::new();
    }

    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            arr.value(idx).to_string()
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            arr.value(idx).to_string()
        }
        _ => {
            // For complex types, use the array's formatter
            format!("{:?}", array.slice(idx, 1))
        }
    }
}
