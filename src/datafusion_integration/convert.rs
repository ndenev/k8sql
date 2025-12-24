// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Conversion utilities for K8s JSON data to Arrow RecordBatches
//!
//! This module converts Kubernetes API JSON responses into Arrow RecordBatches
//! suitable for DataFusion query processing. Key features:
//!
//! - Labels and annotations use native Arrow Map<Utf8, Utf8> types
//! - Spec and status columns are stored as UTF8 JSON strings
//! - Access nested fields using json_get_* functions or PostgreSQL operators (->>, ->)
//! - Other columns (name, namespace, etc.) are simple Utf8 strings

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, MapBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::error::Result;

use crate::kubernetes::discovery::{ColumnDef, ResourceInfo, generate_schema};

// NOTE: spec/status columns are stored as UTF8 JSON strings rather than native Arrow structs.
// This is because DataFusion's query planner needs the schema at planning time, but K8s
// resources have dynamic schemas that vary per-resource-type and are only known at scan time.
// Use json_get_* functions or PostgreSQL operators (->>, ->) for nested field access.
// Example: SELECT json_get_str(status, 'phase') as phase FROM pods

/// Convert our ColumnDef schema to Arrow Schema for TableProvider
pub fn to_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let data_type = match col.name.as_str() {
                "labels" | "annotations" => {
                    let entries_field = Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ])),
                        false,
                    );
                    DataType::Map(Arc::new(entries_field), false)
                }
                _ => DataType::Utf8,
            };
            Field::new(&col.name, data_type, true)
        })
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
        // Labels and annotations use native Map<Utf8, Utf8>
        "labels" | "annotations" => build_map_column(col, resources),

        // Special case: _cluster is injected, not from JSON
        "_cluster" => build_cluster_column(cluster, resources.len()),

        // All other columns are simple strings (including spec/status as JSON)
        _ => build_string_column(col, resources),
    }
}

/// Build a Map<Utf8, Utf8> column for labels/annotations
fn build_map_column(col: &ColumnDef, resources: &[serde_json::Value]) -> Result<(ArrayRef, Field)> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for resource in resources {
        let json_value = get_nested_value(resource, col.json_path.as_deref().unwrap_or(&col.name));
        if let serde_json::Value::Object(map) = json_value {
            for (key, value) in map {
                builder.keys().append_value(&key);
                match value {
                    serde_json::Value::String(s) => builder.values().append_value(&s),
                    serde_json::Value::Null => builder.values().append_null(),
                    other => builder.values().append_value(other.to_string()),
                }
            }
            builder.append(true)?;
        } else {
            // Null or non-object: append empty map
            builder.append(true)?;
        }
    }

    let array = Arc::new(builder.finish()) as ArrayRef;
    let field = Field::new(
        &col.name,
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        ),
        true,
    );

    Ok((array, field))
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
        let value = extract_field_value(resource, &col.name, col.json_path.as_deref());
        builder.append_value(&value);
    }

    let array = Arc::new(builder.finish()) as ArrayRef;
    let field = Field::new(&col.name, DataType::Utf8, true);
    Ok((array, field))
}

/// Extract a field value from a JSON resource
fn extract_field_value(
    resource: &serde_json::Value,
    field_name: &str,
    json_path: Option<&str>,
) -> String {
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
fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_default()
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
