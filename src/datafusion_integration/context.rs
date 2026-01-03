// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion SessionContext setup for k8sql

use std::sync::Arc;

use std::collections::HashSet;

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;

use crate::kubernetes::K8sClientPool;
use crate::output::QueryResult;

use super::preprocess::{preprocess_sql, validate_read_only};
use super::provider::K8sTableProvider;

/// Table information with native types (for data layer)
/// Formatting is done in the presentation layer (output module)
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_name: String,
    pub aliases: Vec<String>, // Keep as Vec (not joined)
    pub group: String,
    pub version: String,
    pub kind: String,
    pub scope: kube::discovery::Scope, // Keep as enum (not string)
    pub is_core: bool,                 // Keep as bool (not "core"/"crd")
}

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
        // Use "postgres" catalog and "public" schema for PostgreSQL client compatibility
        // The _cluster column handles multi-cluster differentiation
        let config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("postgres", "public");
        let mut ctx = SessionContext::new_with_config(config);

        // Register JSON functions for querying spec/status fields
        datafusion_functions_json::register_all(&mut ctx)?;

        let registry = pool.get_registry(None).await?;
        let tables = registry.list_tables();

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

            // Register aliases (ignore conflicts)
            for alias in &info.aliases {
                let alias_provider = K8sTableProvider::new(info.clone(), Arc::clone(pool));
                let _ = ctx.register_table(alias, Arc::new(alias_provider));
            }
        }

        Ok(ctx)
    }

    /// Execute a SQL query and return the results as Arrow RecordBatches
    pub async fn execute_sql(&self, sql: &str) -> DFResult<Vec<RecordBatch>> {
        validate_read_only(sql)
            .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?;

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

    /// Refresh the registered tables (call after context switch)
    pub async fn refresh_tables(&mut self) -> anyhow::Result<()> {
        self.ctx = Self::create_session_context(&self.pool).await?;
        Ok(())
    }

    /// Consume this wrapper and return the underlying DataFusion SessionContext
    pub fn into_session_context(self) -> SessionContext {
        self.ctx
    }

    /// List available tables with full metadata (raw data, not formatted)
    /// Returns Vec of TableInfo with native types for presentation layer to format
    pub async fn list_tables_with_metadata(&self) -> Vec<TableInfo> {
        if let Ok(registry) = self.pool.get_registry(None).await {
            registry
                .list_tables()
                .into_iter()
                .map(|info| TableInfo {
                    table_name: info.table_name.clone(),
                    aliases: info.aliases.clone(),
                    group: info.group.clone(),
                    version: info.version.clone(),
                    kind: info.api_resource.kind.clone(),
                    scope: info.capabilities.scope.clone(),
                    is_core: info.is_core,
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

    /// Get the signature for a scalar UDF by name
    pub fn get_udf_signature(&self, name: &str) -> Option<datafusion::logical_expr::Signature> {
        self.ctx.udf(name).ok().map(|udf| udf.signature().clone())
    }

    /// Get the signature for an aggregate UDF by name
    pub fn get_udaf_signature(&self, name: &str) -> Option<datafusion::logical_expr::Signature> {
        self.ctx
            .udaf(name)
            .ok()
            .map(|udaf| udaf.signature().clone())
    }

    /// Get the signature for a window UDF by name
    pub fn get_udwf_signature(&self, name: &str) -> Option<datafusion::logical_expr::Signature> {
        self.ctx
            .udwf(name)
            .ok()
            .map(|udwf| udwf.signature().clone())
    }
}

/// Convert an Arrow array value at a given index to a string
fn array_value_to_string(array: &datafusion::arrow::array::ArrayRef, idx: usize) -> String {
    use datafusion::arrow::array::*;
    use datafusion::arrow::datatypes::DataType;

    // Helper macro to reduce boilerplate for simple downcast-and-format cases
    macro_rules! downcast_format {
        ($arr_type:ty) => {
            array
                .as_any()
                .downcast_ref::<$arr_type>()
                .unwrap()
                .value(idx)
                .to_string()
        };
    }

    if array.is_null(idx) {
        return String::new();
    }

    match array.data_type() {
        DataType::Utf8 => downcast_format!(StringArray),
        DataType::LargeUtf8 => downcast_format!(LargeStringArray),
        DataType::Int8 => downcast_format!(Int8Array),
        DataType::Int16 => downcast_format!(Int16Array),
        DataType::Int32 => downcast_format!(Int32Array),
        DataType::Int64 => downcast_format!(Int64Array),
        DataType::UInt8 => downcast_format!(UInt8Array),
        DataType::UInt16 => downcast_format!(UInt16Array),
        DataType::UInt32 => downcast_format!(UInt32Array),
        DataType::UInt64 => downcast_format!(UInt64Array),
        DataType::Float32 => downcast_format!(Float32Array),
        DataType::Float64 => downcast_format!(Float64Array),
        DataType::Boolean => downcast_format!(BooleanArray),
        DataType::Timestamp(unit, _) => {
            use datafusion::arrow::datatypes::TimeUnit;

            // We only use Millisecond timestamps, but handle gracefully
            match unit {
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    let millis = arr.value(idx);
                    // Format as ISO 8601 / RFC 3339
                    let secs = millis / 1000;
                    let nsecs = ((millis % 1000) * 1_000_000) as u32;
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs) {
                        dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
                    } else {
                        millis.to_string()
                    }
                }
                _ => format!("{:?}", array.slice(idx, 1)),
            }
        }
        _ => {
            // For complex types, use the array's formatter
            format!("{:?}", array.slice(idx, 1))
        }
    }
}
