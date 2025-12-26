// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! SQL preprocessing for k8sql
//!
//! Transforms k8sql-specific syntax into standard SQL that DataFusion can process.
//!
//! ## Transformations
//!
//! - **Dot notation for labels/annotations**: `labels.app = 'nginx'` becomes
//!   `json_get_str(labels, 'app') = 'nginx'`
//! - **JSON arrow precedence fix**: `col->>'key' = 'val'` becomes
//!   `(col->>'key') = 'val'` to work around DataFusion parser precedence
//!
//! ## Implementation
//!
//! Uses AST-based transformation via sqlparser for correctness:
//! - No false positives in string literals
//! - Proper handling of CTEs, subqueries, complex expressions
//!
//! The `->>` precedence fix uses regex before parsing because the issue
//! occurs during parsing itself.
//!
//! ## Limitations
//!
//! Label keys containing `/` (like `app.kubernetes.io/name`) cannot use dot notation
//! because `/` is parsed as division. Use explicit syntax instead:
//! ```sql
//! -- This won't work: labels.app.kubernetes.io/name = 'foo'
//! -- Use this instead:
//! json_get_str(labels, 'app.kubernetes.io/name') = 'foo'
//! ```

use core::ops::ControlFlow;
use regex::Regex;
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, Value, visit_expressions_mut,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::LazyLock;

/// Columns that support dot-notation access (transformed to json_get_str)
const JSON_DOT_COLUMNS: &[&str] = &["labels", "annotations"];

/// Transform `labels.key` and `annotations.key` expressions to `json_get_str(col, 'key')`
fn transform_dot_notation(expr: &mut Expr) -> ControlFlow<()> {
    // Check for CompoundIdentifier like `labels.app` or `labels.app.kubernetes.io`
    if let Expr::CompoundIdentifier(parts) = expr
        && parts.len() >= 2
    {
        let first = parts[0].value.to_lowercase();
        if JSON_DOT_COLUMNS.contains(&first.as_str()) {
            // Build the key from remaining parts: app.kubernetes.io
            let key = parts[1..]
                .iter()
                .map(|p| p.value.clone())
                .collect::<Vec<_>>()
                .join(".");

            // Transform to: json_get_str(labels, 'key')
            *expr = build_json_get_str(&first, &key);
        }
    }
    ControlFlow::Continue(())
}

/// Build an Expr representing `json_get_str(column, 'key')`
fn build_json_get_str(column: &str, key: &str) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new("json_get_str")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(Ident::new(column)))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                    Value::SingleQuotedString(key.to_string()).into(),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// Regex to fix ->> operator precedence when followed by comparison operators.
/// This is a workaround for DataFusion's parser incorrectly parsing:
///   `col->>'key' = 'val'` as `col->>('key' = 'val')`
/// We wrap it as `(col->>'key') = 'val'`
///
/// Note: This must be done at string level before parsing because the parser
/// gets the precedence wrong.
static JSON_ARROW_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)((?:\w+\.)?\w+)\s*->>\s*'([^']+)'\s*(=|!=|<>|<=|>=|<|>|NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE)"#).unwrap()
});

/// Pre-parse fix for ->> operator precedence (must be done before AST parsing)
fn fix_json_arrow_precedence(sql: &str) -> String {
    JSON_ARROW_PATTERN
        .replace_all(sql, |caps: &regex::Captures| {
            let column = &caps[1];
            let key = &caps[2];
            let operator = &caps[3];
            format!("({}->>'{}') {}", column, key, operator)
        })
        .into_owned()
}

/// Preprocess SQL to convert k8sql-specific syntax to standard SQL
///
/// Transformations:
/// - `labels.app = 'nginx'` -> `json_get_str(labels, 'app') = 'nginx'`
/// - `annotations.key = 'value'` -> `json_get_str(annotations, 'key') = 'value'`
/// - `col->>'key' = 'value'` -> `(col->>'key') = 'value'` (fixes operator precedence)
///
/// Uses AST-based transformation for labels/annotations (correct handling of
/// string literals, complex queries, etc.) and regex for ->> precedence fix
/// (must be done before parsing due to DataFusion parser behavior).
pub fn preprocess_sql(sql: &str) -> String {
    // First fix ->> precedence at string level (before parsing)
    let sql = fix_json_arrow_precedence(sql);

    // Try to parse and transform via AST
    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, &sql) {
        Ok(mut statements) => {
            // Transform each statement's expressions
            for stmt in &mut statements {
                let _ = visit_expressions_mut(stmt, transform_dot_notation);
            }

            // Convert back to SQL string
            statements
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        }
        Err(_) => {
            // If parsing fails, return the ->> fixed version
            // (DataFusion will report the actual parse error)
            sql
        }
    }
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
        // AST normalization converts != to <> (semantically equivalent)
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'env') <> 'prod'"
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
    fn test_label_with_dots() {
        // Dots in label keys work with dot notation
        let sql = "SELECT * FROM pods WHERE labels.app.kubernetes.io = 'test'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'app.kubernetes.io') = 'test'"
        );
    }

    #[test]
    fn test_label_with_slash_requires_explicit_syntax() {
        // Labels with '/' in the key (like app.kubernetes.io/name) cannot use dot notation
        // because '/' is parsed as division. Users must use explicit json_get_str syntax.
        let sql =
            "SELECT * FROM pods WHERE json_get_str(labels, 'app.kubernetes.io/name') = 'test'";
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
        assert_eq!(result, "SELECT * FROM pods WHERE namespace = 'default'");
    }

    #[test]
    fn test_json_arrow_precedence_equals() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' = 'Running'";
        let result = preprocess_sql(sql);
        // AST serialization adds spaces around ->>
        assert!(result.contains("(status ->> 'phase') = 'Running'"));
    }

    #[test]
    fn test_json_arrow_precedence_not_equals() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' != 'Running'";
        let result = preprocess_sql(sql);
        // AST normalizes != to <> and adds spaces around ->>
        assert!(result.contains("(status ->> 'phase') <> 'Running'"));
    }

    #[test]
    fn test_json_arrow_precedence_greater_than() {
        let sql = "SELECT * FROM pods WHERE spec->>'replicas' > 1";
        let result = preprocess_sql(sql);
        // AST adds spaces around ->>
        assert!(result.contains("(spec ->> 'replicas') >"));
    }

    #[test]
    fn test_json_arrow_in_select_not_modified() {
        // ->> in SELECT without comparison should not be modified
        let sql = "SELECT status->>'phase' FROM pods";
        let result = preprocess_sql(sql);
        assert_eq!(result, "SELECT status ->> 'phase' FROM pods");
    }

    #[test]
    fn test_label_like() {
        let sql = "SELECT * FROM pods WHERE labels.app LIKE 'nginx%'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE json_get_str(labels, 'app') LIKE 'nginx%'"
        );
    }

    #[test]
    fn test_label_in_string_literal_not_transformed() {
        // labels inside a string should NOT be transformed
        let sql = "SELECT * FROM pods WHERE name = 'labels.app'";
        let result = preprocess_sql(sql);
        assert_eq!(result, "SELECT * FROM pods WHERE name = 'labels.app'");
    }

    #[test]
    fn test_subquery_with_labels() {
        let sql = "SELECT * FROM pods WHERE namespace IN (SELECT namespace FROM deployments WHERE labels.app = 'nginx')";
        let result = preprocess_sql(sql);
        assert!(result.contains("json_get_str(labels, 'app')"));
    }

    #[test]
    fn test_cte_with_labels() {
        let sql = "WITH nginx_pods AS (SELECT * FROM pods WHERE labels.app = 'nginx') SELECT * FROM nginx_pods";
        let result = preprocess_sql(sql);
        assert!(result.contains("json_get_str(labels, 'app')"));
    }

    #[test]
    fn test_label_in_select_clause() {
        let sql = "SELECT labels.app, name FROM pods";
        let result = preprocess_sql(sql);
        assert!(result.contains("json_get_str(labels, 'app')"));
    }
}
