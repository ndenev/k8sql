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
    use datafusion::arrow::array::Array;

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

    #[test]
    fn test_get_nested_value_array_index() {
        let json = serde_json::json!({
            "containers": [
                {"name": "nginx", "image": "nginx:latest"},
                {"name": "sidecar", "image": "sidecar:v1"}
            ]
        });

        // Access array by index
        let first_container = get_nested_value(&json, "containers.0");
        assert_eq!(first_container["name"], "nginx");

        let second_name = get_nested_value(&json, "containers.1.name");
        assert_eq!(second_name, "sidecar");

        // Out of bounds returns null
        let oob = get_nested_value(&json, "containers.10");
        assert_eq!(oob, serde_json::Value::Null);

        // Non-numeric index on array returns null
        let invalid = get_nested_value(&json, "containers.invalid");
        assert_eq!(invalid, serde_json::Value::Null);
    }

    #[test]
    fn test_get_nested_value_deeply_nested() {
        let json = serde_json::json!({
            "spec": {
                "containers": [
                    {
                        "resources": {
                            "limits": {
                                "cpu": "100m",
                                "memory": "128Mi"
                            }
                        }
                    }
                ]
            }
        });

        let cpu = get_nested_value(&json, "spec.containers.0.resources.limits.cpu");
        assert_eq!(cpu, "100m");
    }

    #[test]
    fn test_get_nested_value_primitive_path() {
        // Trying to navigate into a primitive value
        let json = serde_json::json!({
            "name": "test"
        });

        let result = get_nested_value(&json, "name.nested");
        assert_eq!(result, serde_json::Value::Null);
    }

    #[test]
    fn test_format_json_value_null() {
        assert_eq!(format_json_value(&serde_json::Value::Null), None);
    }

    #[test]
    fn test_format_json_value_bool() {
        assert_eq!(
            format_json_value(&serde_json::json!(true)),
            Some("true".to_string())
        );
        assert_eq!(
            format_json_value(&serde_json::json!(false)),
            Some("false".to_string())
        );
    }

    #[test]
    fn test_format_json_value_number() {
        assert_eq!(
            format_json_value(&serde_json::json!(42)),
            Some("42".to_string())
        );
        assert_eq!(
            format_json_value(&serde_json::json!(3.14)),
            Some("3.14".to_string())
        );
    }

    #[test]
    fn test_format_json_value_string() {
        assert_eq!(
            format_json_value(&serde_json::json!("hello")),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_format_json_value_object() {
        let obj = serde_json::json!({"key": "value"});
        let result = format_json_value(&obj).unwrap();
        // Should be valid JSON string
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn test_format_json_value_array() {
        let arr = serde_json::json!([1, 2, 3]);
        let result = format_json_value(&arr).unwrap();
        // Should be valid JSON string
        let parsed: Vec<i32> = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed, vec![1, 2, 3]);
    }

    #[test]
    fn test_to_arrow_schema() {
        let columns = vec![
            ColumnDef {
                name: "_cluster".to_string(),
                data_type: "text".to_string(),
                json_path: None,
                description: "Cluster name".to_string(),
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: "text".to_string(),
                json_path: Some("metadata.name".to_string()),
                description: "Resource name".to_string(),
            },
            ColumnDef {
                name: "namespace".to_string(),
                data_type: "text".to_string(),
                json_path: Some("metadata.namespace".to_string()),
                description: "Namespace".to_string(),
            },
        ];

        let schema = to_arrow_schema(&columns);
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "_cluster");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "namespace");
        // All should be Utf8
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_extract_field_value_with_json_path() {
        let resource = serde_json::json!({
            "metadata": {
                "name": "nginx-pod"
            }
        });

        let result = extract_field_value(&resource, "name", Some("metadata.name"));
        assert_eq!(result, Some("nginx-pod".to_string()));
    }

    #[test]
    fn test_extract_field_value_without_json_path() {
        let resource = serde_json::json!({
            "name": "direct-name"
        });

        let result = extract_field_value(&resource, "name", None);
        assert_eq!(result, Some("direct-name".to_string()));
    }

    #[test]
    fn test_extract_field_value_missing() {
        let resource = serde_json::json!({});

        let result = extract_field_value(&resource, "name", Some("metadata.name"));
        assert_eq!(result, None);
    }

    #[test]
    fn test_build_cluster_column() {
        let (array, field) = build_cluster_column("test-cluster", 3).unwrap();

        assert_eq!(field.name(), "_cluster");
        assert_eq!(field.data_type(), &DataType::Utf8);

        let string_array = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "test-cluster");
        assert_eq!(string_array.value(1), "test-cluster");
        assert_eq!(string_array.value(2), "test-cluster");
    }

    #[test]
    fn test_build_string_column() {
        let col = ColumnDef {
            name: "name".to_string(),
            data_type: "text".to_string(),
            json_path: Some("metadata.name".to_string()),
            description: "Resource name".to_string(),
        };

        let resources = vec![
            serde_json::json!({"metadata": {"name": "nginx"}}),
            serde_json::json!({"metadata": {"name": "redis"}}),
            serde_json::json!({"metadata": {}}), // missing name
        ];

        let (array, field) = build_string_column(&col, &resources).unwrap();

        assert_eq!(field.name(), "name");
        assert_eq!(field.data_type(), &DataType::Utf8);

        let string_array = array
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "nginx");
        assert_eq!(string_array.value(1), "redis");
        assert!(string_array.is_null(2)); // missing value should be null
    }
}
