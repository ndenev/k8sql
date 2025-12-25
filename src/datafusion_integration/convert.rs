// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Conversion utilities for K8s JSON data to Arrow RecordBatches
//!
//! This module converts Kubernetes API JSON responses into Arrow RecordBatches
//! suitable for DataFusion query processing. Key features:
//!
//! - All columns are stored as UTF8 strings for PostgreSQL wire protocol compatibility
//! - Labels, annotations, spec, and status are stored as JSON strings
//! - Access nested fields using json_get_* functions or PostgreSQL operators (->>, ->)
//! - Example: SELECT labels->>'app' FROM pods WHERE labels->>'app' = 'nginx'

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;

use crate::kubernetes::discovery::{ColumnDef, ResourceInfo, generate_schema};

// NOTE: spec/status columns are stored as UTF8 JSON strings rather than native Arrow structs.
// This is because DataFusion's query planner needs the schema at planning time, but K8s
// resources have dynamic schemas that vary per-resource-type and are only known at scan time.
// Use json_get_* functions or PostgreSQL operators (->>, ->) for nested field access.
// Example: SELECT json_get_str(status, 'phase') as phase FROM pods

/// Convert our ColumnDef schema to Arrow Schema for TableProvider
/// All columns are stored as Utf8 strings (including labels/annotations as JSON)
pub fn to_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, DataType::Utf8, true))
        .collect();

    Arc::new(Schema::new(fields))
}

/// Convert K8s JSON resources to Arrow RecordBatch
///
/// Takes the cluster name, resource info, and JSON values from the K8s API
/// and converts them to an Arrow RecordBatch suitable for DataFusion.
pub fn json_to_record_batch(
    cluster: &str,
    resource_info: &ResourceInfo,
    resources: Vec<serde_json::Value>,
) -> Result<RecordBatch> {
    let columns = generate_schema(resource_info);

    if resources.is_empty() {
        let schema = to_arrow_schema(&columns);
        return Ok(RecordBatch::new_empty(schema));
    }

    // Build arrays for each column
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut fields: Vec<Field> = Vec::with_capacity(columns.len());

    for col in &columns {
        let (array, field) = build_column_array(cluster, col, &resources)?;
        arrays.push(array);
        fields.push(field);
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

/// Build an Arrow array for a single column
fn build_column_array(
    cluster: &str,
    col: &ColumnDef,
    resources: &[serde_json::Value],
) -> Result<(ArrayRef, Field)> {
    match col.name.as_str() {
        // Special case: _cluster is injected, not from JSON
        "_cluster" => build_cluster_column(cluster, resources.len()),

        // All other columns are strings (including labels/annotations/spec/status as JSON)
        _ => build_string_column(col, resources),
    }
}

/// Build the _cluster column (constant value for all rows)
fn build_cluster_column(cluster: &str, num_rows: usize) -> Result<(ArrayRef, Field)> {
    let mut builder = StringBuilder::with_capacity(num_rows, cluster.len() * num_rows);
    for _ in 0..num_rows {
        builder.append_value(cluster);
    }
    let array = Arc::new(builder.finish()) as ArrayRef;
    let field = Field::new("_cluster", DataType::Utf8, true);
    Ok((array, field))
}

/// Build a simple string column by extracting values from JSON
fn build_string_column(
    col: &ColumnDef,
    resources: &[serde_json::Value],
) -> Result<(ArrayRef, Field)> {
    let mut builder = StringBuilder::new();

    for resource in resources {
        match extract_field_value(resource, &col.name, col.json_path.as_deref()) {
            Some(value) => builder.append_value(&value),
            None => builder.append_null(),
        }
    }

    let array = Arc::new(builder.finish()) as ArrayRef;
    let field = Field::new(&col.name, DataType::Utf8, true);
    Ok((array, field))
}

/// Extract a field value from a JSON resource
/// Returns None for null/missing values to preserve SQL NULL semantics
fn extract_field_value(
    resource: &serde_json::Value,
    field_name: &str,
    json_path: Option<&str>,
) -> Option<String> {
    let path = json_path.unwrap_or(field_name);
    let value = get_nested_value(resource, path);
    format_json_value(&value)
}

/// Navigate a dotted path in a JSON value
fn get_nested_value(value: &serde_json::Value, path: &str) -> serde_json::Value {
    let mut current = value;

    for part in path.split('.') {
        current = match current {
            serde_json::Value::Object(map) => map.get(part).unwrap_or(&serde_json::Value::Null),
            serde_json::Value::Array(arr) => {
                if let Ok(idx) = part.parse::<usize>() {
                    arr.get(idx).unwrap_or(&serde_json::Value::Null)
                } else {
                    &serde_json::Value::Null
                }
            }
            _ => &serde_json::Value::Null,
        };
    }

    current.clone()
}

/// Format a JSON value as a string for storage/display
/// Returns None for null values to preserve SQL NULL semantics
fn format_json_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Some(serde_json::to_string(value).unwrap_or_default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_nested_value() {
        let json = serde_json::json!({
            "metadata": {
                "name": "test-pod",
                "namespace": "default"
            },
            "status": {
                "phase": "Running"
            }
        });

        assert_eq!(
            get_nested_value(&json, "metadata.name"),
            serde_json::json!("test-pod")
        );
        assert_eq!(
            get_nested_value(&json, "status.phase"),
            serde_json::json!("Running")
        );
        assert_eq!(
            get_nested_value(&json, "nonexistent"),
            serde_json::Value::Null
        );
    }
}
