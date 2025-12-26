// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! SQL preprocessing for k8sql
//!
//! Fixes DataFusion parser quirks before query execution.
//!
//! ## Transformations
//!
//! - **JSON arrow precedence fix**: `col->>'key' = 'val'` becomes
//!   `(col->>'key') = 'val'` to work around DataFusion parser precedence
//!
//! ## JSON Field Access
//!
//! Use PostgreSQL-style JSON operators for accessing nested fields:
//! ```sql
//! -- Access label value
//! SELECT * FROM pods WHERE labels->>'app' = 'nginx'
//!
//! -- Access nested status field
//! SELECT status->>'phase' FROM pods
//! ```

use regex::Regex;
use std::sync::LazyLock;

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

/// Pre-parse fix for ->> operator precedence
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

/// Preprocess SQL to fix parser quirks
///
/// Currently only fixes ->> operator precedence:
/// - `col->>'key' = 'value'` -> `(col->>'key') = 'value'`
pub fn preprocess_sql(sql: &str) -> String {
    fix_json_arrow_precedence(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_json_arrow_precedence_greater_than() {
        let sql = "SELECT * FROM pods WHERE spec->>'replicas' > 1";
        let result = preprocess_sql(sql);
        assert!(result.contains("(spec->>'replicas') >"));
    }

    #[test]
    fn test_json_arrow_in_select_not_modified() {
        // ->> in SELECT without comparison should not be modified
        let sql = "SELECT status->>'phase' FROM pods";
        let result = preprocess_sql(sql);
        assert_eq!(result, "SELECT status->>'phase' FROM pods");
    }

    #[test]
    fn test_json_arrow_like() {
        let sql = "SELECT * FROM pods WHERE labels->>'app' LIKE 'nginx%'";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (labels->>'app') LIKE 'nginx%'"
        );
    }

    #[test]
    fn test_no_arrow_unchanged() {
        let sql = "SELECT * FROM pods WHERE namespace = 'default'";
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn test_multiple_arrows() {
        let sql = "SELECT * FROM pods WHERE labels->>'app' = 'nginx' AND labels->>'env' = 'prod'";
        let result = preprocess_sql(sql);
        assert!(result.contains("(labels->>'app') = 'nginx'"));
        assert!(result.contains("(labels->>'env') = 'prod'"));
    }
}
