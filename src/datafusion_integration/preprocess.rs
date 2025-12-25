// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! SQL preprocessing for k8sql
//!
//! Transforms k8sql-specific syntax into standard SQL that DataFusion can process.

use regex::Regex;
use std::sync::LazyLock;

/// Regex to match `labels.key = 'value'` or `labels.key = "value"` patterns
/// Also matches labels.key != 'value' for inequality
static LABEL_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    // Match: labels.IDENTIFIER operator 'value' or "value"
    // Operators: =, !=, <>
    Regex::new(r#"labels\.([a-zA-Z0-9_.\-/]+)\s*(=|!=|<>)\s*('[^']*'|"[^"]*")"#).unwrap()
});

/// Regex to match `annotations.key = 'value'` patterns
static ANNOTATION_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"annotations\.([a-zA-Z0-9_.\-/]+)\s*(=|!=|<>)\s*('[^']*'|"[^"]*")"#).unwrap()
});

/// Regex to fix ->> operator precedence when followed by comparison operators
/// Matches: column->>'key' = value  and wraps as (column->>'key') = value
/// This fixes DataFusion's incorrect parsing of col->>'key' = 'val' as col->>('key' = 'val')
static JSON_ARROW_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    // Match: identifier->>'key' followed by comparison operator
    // The identifier can include dots for nested access like spec.containers
    Regex::new(r#"(\w+)\s*->>\s*'([^']+)'\s*(=|!=|<>|<=|>=|<|>)"#).unwrap()
});

/// Preprocess SQL to convert k8sql-specific syntax to standard SQL
///
/// Transformations:
/// - `labels.app = 'nginx'` -> `json_get_str(labels, 'app') = 'nginx'`
/// - `annotations.key = 'value'` -> `json_get_str(annotations, 'key') = 'value'`
/// - `col->>'key' = 'value'` -> `(col->>'key') = 'value'` (fixes operator precedence)
///
/// This allows dot-notation access while using DataFusion's JSON functions.
/// The TableProvider can detect json_get_str patterns for K8s API label selector pushdown.
pub fn preprocess_sql(sql: &str) -> String {
    // Fix ->> operator precedence by wrapping in parentheses
    // col->>'key' = 'val' -> (col->>'key') = 'val'
    let result = JSON_ARROW_PATTERN.replace_all(sql, |caps: &regex::Captures| {
        let column = &caps[1];
        let key = &caps[2];
        let operator = &caps[3];
        format!("({}->>'{}') {}", column, key, operator)
    });

    // Replace labels.key patterns with json_get_str(labels, 'key')
    let result = LABEL_PATTERN.replace_all(&result, |caps: &regex::Captures| {
        let key = &caps[1];
        let operator = &caps[2];
        let value = &caps[3];
        format!("json_get_str(labels, '{}') {} {}", key, operator, value)
    });

    // Replace annotations.key patterns with json_get_str(annotations, 'key')
    let result = ANNOTATION_PATTERN.replace_all(&result, |caps: &regex::Captures| {
        let key = &caps[1];
        let operator = &caps[2];
        let value = &caps[3];
        format!(
            "json_get_str(annotations, '{}') {} {}",
            key, operator, value
        )
    });

    result.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_equality() {
        let sql = "SELECT * FROM pods WHERE labels.app = 'nginx'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'app') = 'nginx'"
        );
    }

    #[test]
    fn test_label_inequality() {
        let sql = "SELECT * FROM pods WHERE labels.env != 'prod'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'env') != 'prod'"
        );
    }

    #[test]
    fn test_multiple_labels() {
        let sql = "SELECT * FROM pods WHERE labels.app = 'nginx' AND labels.env = 'prod'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'app') = 'nginx' AND json_get_str(labels, 'env') = 'prod'"
        );
    }

    #[test]
    fn test_label_with_special_chars() {
        let sql = "SELECT * FROM pods WHERE labels.app.kubernetes.io/name = 'test'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'app.kubernetes.io/name') = 'test'"
        );
    }

    #[test]
    fn test_annotation() {
        let sql = "SELECT * FROM pods WHERE annotations.key = 'value'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(annotations, 'key') = 'value'"
        );
    }

    #[test]
    fn test_no_labels() {
        let sql = "SELECT * FROM pods WHERE namespace = 'default'";
        let result = preprocess_sql(sql);
        assert_eq!(sql, result);
    }

    #[test]
    fn test_json_arrow_precedence_equals() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' = 'Running'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (status->>'phase') = 'Running'"
        );
    }

    #[test]
    fn test_json_arrow_precedence_not_equals() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' != 'Running'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (status->>'phase') != 'Running'"
        );
    }

    #[test]
    fn test_json_arrow_precedence_less_than() {
        let sql = "SELECT * FROM pods WHERE spec->>'replicas' > 1";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (spec->>'replicas') > 1"
        );
    }

    #[test]
    fn test_json_arrow_multiple() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' = 'Running' AND spec->>'nodeName' != 'node1'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (status->>'phase') = 'Running' AND (spec->>'nodeName') != 'node1'"
        );
    }

    #[test]
    fn test_json_arrow_in_select_not_modified() {
        // ->> in SELECT without comparison should not be modified
        let sql = "SELECT status->>'phase' FROM pods";
        let result = preprocess_sql(sql);
        assert_eq!(sql, result);
    }
}
