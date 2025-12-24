// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Conversion utilities for K8s JSON data to Arrow RecordBatches

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;

use crate::kubernetes::discovery::{generate_schema, ColumnDef, ResourceInfo};

/// Convert our ColumnDef schema to Arrow Schema
pub fn to_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            // For now, all columns are strings (we can add type inference later)
            Field::new(&col.name, DataType::Utf8, true)
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
    let schema = to_arrow_schema(&columns);

    // Create string builders for each column
    let mut builders: Vec<StringBuilder> = columns.iter().map(|_| StringBuilder::new()).collect();

    // Process each resource
    for resource in &resources {
        for (idx, col) in columns.iter().enumerate() {
            let value = if col.name == "_cluster" {
                cluster.to_string()
            } else {
                extract_field_value(resource, &col.name, col.json_path.as_deref())
            };
            builders[idx].append_value(&value);
        }
    }

    // Build arrays
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|mut b| Arc::new(b.finish()) as ArrayRef)
        .collect();

    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

/// Extract a field value from a JSON resource
fn extract_field_value(
    resource: &serde_json::Value,
    field_name: &str,
    json_path: Option<&str>,
) -> String {
    // Use json_path if provided, otherwise use field_name as path
    let path = json_path.unwrap_or(field_name);

    // Handle nested paths like "metadata.name" or "status.phase"
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
                // Try to parse as index
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

/// Format a JSON value as a string for display
fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(arr) => {
            // Format arrays as comma-separated values or JSON
            if arr.iter().all(|v| v.is_string()) {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                serde_json::to_string(arr).unwrap_or_default()
            }
        }
        serde_json::Value::Object(obj) => {
            // Format objects as key=value pairs or JSON
            let simple: Vec<String> = obj
                .iter()
                .filter_map(|(k, v)| {
                    if let serde_json::Value::String(s) = v {
                        Some(format!("{}={}", k, s))
                    } else {
                        None
                    }
                })
                .collect();

            if simple.len() == obj.len() {
                simple.join(",")
            } else {
                serde_json::to_string(obj).unwrap_or_default()
            }
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
