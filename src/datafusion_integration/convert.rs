// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Conversion utilities for K8s JSON data to Arrow RecordBatches

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, MapBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::error::Result;

use crate::kubernetes::discovery::{generate_schema, ColumnDef, ResourceInfo};

/// Convert our ColumnDef schema to Arrow Schema
pub fn to_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let data_type = match col.name.as_str() {
                // Labels and annotations are Map<Utf8, Utf8> for native access
                "labels" | "annotations" => {
                    // Map type: MapBuilder produces Struct<keys: Utf8, values: Utf8>
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
                // All other columns remain as strings for now
                _ => DataType::Utf8,
            };
            Field::new(&col.name, data_type, true)
        })
        .collect();

    Arc::new(Schema::new(fields))
}

/// Builder enum to handle different column types
enum ColumnBuilder {
    String(StringBuilder),
    Map(MapBuilder<StringBuilder, StringBuilder>),
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

    // Create appropriate builders for each column type
    let mut builders: Vec<ColumnBuilder> = columns
        .iter()
        .map(|col| match col.name.as_str() {
            "labels" | "annotations" => {
                ColumnBuilder::Map(MapBuilder::new(None, StringBuilder::new(), StringBuilder::new()))
            }
            _ => ColumnBuilder::String(StringBuilder::new()),
        })
        .collect();

    // Process each resource
    for resource in &resources {
        for (idx, col) in columns.iter().enumerate() {
            match &mut builders[idx] {
                ColumnBuilder::String(builder) => {
                    let value = if col.name == "_cluster" {
                        cluster.to_string()
                    } else {
                        extract_field_value(resource, &col.name, col.json_path.as_deref())
                    };
                    builder.append_value(&value);
                }
                ColumnBuilder::Map(builder) => {
                    // Extract JSON object and convert to Map entries
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
            }
        }
    }

    // Build arrays from builders
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .map(|builder| match builder {
            ColumnBuilder::String(mut b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Map(mut b) => Arc::new(b.finish()) as ArrayRef,
        })
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

/// Format a JSON value as a string for storage/display
/// Arrays and objects are kept as JSON strings to enable JSON function queries
fn format_json_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        // Keep arrays and objects as JSON strings for JSON function compatibility
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
