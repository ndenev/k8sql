// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion SessionContext setup for k8sql

use std::sync::Arc;

use std::collections::HashSet;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::kubernetes::K8sClientPool;
use crate::output::QueryResult;

use super::preprocess::preprocess_sql;
use super::provider::K8sTableProvider;

/// A wrapper around DataFusion's SessionContext configured for K8s queries
#[derive(Clone)]
pub struct K8sSessionContext {
    ctx: SessionContext,
    pool: Arc<K8sClientPool>,
}

impl K8sSessionContext {
    /// Create a new K8sSessionContext with all discovered K8s resources registered as tables
    pub async fn new(pool: Arc<K8sClientPool>) -> anyhow::Result<Self> {
        let ctx = Self::create_session_context(&pool).await?;
        Ok(Self { ctx, pool })
    }

    /// Create and configure a SessionContext with all K8s tables registered
    async fn create_session_context(pool: &Arc<K8sClientPool>) -> anyhow::Result<SessionContext> {
        // Get the current Kubernetes context name for the catalog
        let cluster_name = pool
            .current_context()
            .await
            .unwrap_or_else(|_| "k8s".to_string());

        // Enable information_schema and use cluster name as catalog
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema(&cluster_name, "default");
        let mut ctx = SessionContext::new_with_config(config);

        // Register JSON functions for querying spec/status fields
        datafusion_functions_json::register_all(&mut ctx)?;

        // Get the resource registry and register each resource as a table
        let registry = pool.get_registry(None).await?;
        let tables = registry.list_tables();

        // Report table registration start
        pool.progress().registering_tables(tables.len());

        for info in tables {
            let provider = K8sTableProvider::new(info.clone(), Arc::clone(pool));

            // Register with the primary table name (ignore conflicts from duplicate CRDs)
            if let Err(e) = ctx.register_table(&info.table_name, Arc::new(provider)) {
                tracing::debug!(
                    "Skipping duplicate table '{}' from {}/{}: {}",
                    info.table_name,
                    info.group,
                    info.version,
                    e
                );
            }

            // Also register aliases (ignore conflicts)
            for alias in &info.aliases {
                let alias_provider = K8sTableProvider::new(info.clone(), Arc::clone(pool));
                let _ = ctx.register_table(alias, Arc::new(alias_provider));
            }
        }

        Ok(ctx)
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
        self.ctx = Self::create_session_context(&self.pool).await?;
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

    /// List available tables (primary names only, no aliases)
    #[allow(dead_code)]
    pub fn list_tables(&self) -> Vec<String> {
        // Use blocking runtime to get registry info
        let pool = Arc::clone(&self.pool);
        let handle = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            s.spawn(|| {
                handle.block_on(async {
                    if let Ok(registry) = pool.get_registry(None).await {
                        registry
                            .list_tables()
                            .into_iter()
                            .map(|info| info.table_name.clone())
                            .collect()
                    } else {
                        vec![]
                    }
                })
            })
            .join()
            .unwrap_or_default()
        })
    }

    /// List available tables with their aliases
    /// Returns Vec of (table_name, aliases_string)
    pub async fn list_tables_with_aliases(&self) -> Vec<(String, String)> {
        if let Ok(registry) = self.pool.get_registry(None).await {
            registry
                .list_tables()
                .into_iter()
                .map(|info| {
                    let aliases = if info.aliases.is_empty() {
                        String::new()
                    } else {
                        info.aliases.join(", ")
                    };
                    (info.table_name.clone(), aliases)
                })
                .collect()
        } else {
            vec![]
        }
    }

    /// List all registered functions (scalar, aggregate, and window)
    pub fn list_functions(&self) -> HashSet<String> {
        let mut functions = HashSet::new();

        // Scalar functions (json_get_str, etc.)
        functions.extend(self.ctx.udfs());

        // Aggregate functions (COUNT, SUM, etc.)
        functions.extend(self.ctx.udafs());

        // Window functions (ROW_NUMBER, etc.)
        functions.extend(self.ctx.udwfs());

        functions
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
