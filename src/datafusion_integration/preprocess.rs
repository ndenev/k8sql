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
//! - **Chained JSON arrows**: `spec->'selector'->>'app'` becomes
//!   `json_get_str(json_get(spec, 'selector'), 'app')` for PostgreSQL compatibility
//!
//! ## JSON Field Access
//!
//! Use PostgreSQL-style JSON operators for accessing nested fields:
//! ```sql
//! -- Access label value
//! SELECT * FROM pods WHERE labels->>'app' = 'nginx'
//!
//! -- Access nested field (chained)
//! SELECT spec->'selector'->>'app' FROM services
//! ```

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use regex::Regex;
use std::sync::LazyLock;

/// Regex to match JSON arrow operator sequences
/// Matches patterns like: column->'key' or column->'k1'->'k2'->>'k3'
static JSON_ARROW_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)((?:\w+\.)?\w+)(?:\s*->>?\s*'[^']+')+").unwrap());

/// Extract arrow operations from a matched string
/// Returns Vec<(key, is_text)> where is_text indicates ->> vs ->
fn extract_arrow_operations(s: &str) -> Vec<(&str, bool)> {
    let arrow_regex = Regex::new(r"->>?\s*'([^']+)'").unwrap();
    arrow_regex
        .captures_iter(s)
        .map(|c| {
            let key = c.get(1).unwrap().as_str();
            let is_text = s[c.get(0).unwrap().start()..].starts_with("->>");
            (key, is_text)
        })
        .collect()
}

/// Convert arrow operations to nested json_get function calls
fn convert_to_json_get_chain(base_column: &str, arrows: &[(&str, bool)]) -> String {
    let mut result = base_column.to_string();
    for (i, (key, is_text)) in arrows.iter().enumerate() {
        let is_last = i == arrows.len() - 1;
        if is_last && *is_text {
            // Last operation with ->> uses json_get_str
            result = format!("json_get_str({}, '{}')", result, key);
        } else {
            // Intermediate operations use json_get
            result = format!("json_get({}, '{}')", result, key);
        }
    }
    result
}

/// Process JSON arrow operators - convert chains to json_get, wrap singles with parens
///
/// This function handles both:
/// 1. Chained arrows (2+): spec->'selector'->>'app' → json_get_str(json_get(spec, 'selector'), 'app')
/// 2. Single arrows with comparison: labels->>'app' = 'x' → (labels->>'app') = 'x'
///
/// The precedence fix is needed due to DataFusion parser bug:
/// https://github.com/apache/datafusion-sqlparser-rs/issues/814
fn process_json_arrows(sql: &str) -> String {
    // First pass: convert chained arrows to json_get functions
    let sql = JSON_ARROW_PATTERN
        .replace_all(sql, |caps: &regex::Captures| {
            let full_match = caps.get(0).unwrap().as_str();
            let base_column = &caps[1];
            let arrows = extract_arrow_operations(full_match);

            if arrows.len() >= 2 {
                // Chain: convert to nested json_get calls
                convert_to_json_get_chain(base_column, &arrows)
            } else {
                // Single arrow: keep as-is for now (will be handled in second pass)
                full_match.to_string()
            }
        })
        .into_owned();

    // Second pass: wrap remaining single arrows before comparison operators
    let comparison_pattern = Regex::new(
        r#"(?i)((?:\w+\.)?\w+)\s*->>?\s*'([^']+)'\s*(=|!=|<>|<=|>=|<|>|NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s+IN|IN)"#,
    )
    .unwrap();

    comparison_pattern
        .replace_all(&sql, |caps: &regex::Captures| {
            let column = &caps[1];
            let key = &caps[2];
            let operator = &caps[3];
            format!("({}->>'{}') {}", column, key, operator)
        })
        .into_owned()
}

/// Preprocess SQL to fix parser quirks
///
/// Applies the following transformations:
/// 1. Converts chained JSON arrows to json_get functions:
///    - `spec->'selector'->>'app'` -> `json_get_str(json_get(spec, 'selector'), 'app')`
/// 2. Fixes ->> operator precedence:
///    - `col->>'key' = 'value'` -> `(col->>'key') = 'value'`
pub fn preprocess_sql(sql: &str) -> String {
    process_json_arrows(sql)
}

/// Validate that the SQL statement is read-only
///
/// k8sql is a read-only tool for querying Kubernetes resources.
/// This function rejects any DDL (CREATE, DROP, ALTER) or DML (INSERT, UPDATE, DELETE)
/// statements with a clear error message.
pub fn validate_read_only(sql: &str) -> anyhow::Result<()> {
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| anyhow::anyhow!("SQL parse error: {}", e))?;

    for stmt in statements {
        match stmt {
            // Allowed read-only statements
            Statement::Query(_) => {}
            Statement::ShowTables { .. } => {}
            Statement::ShowDatabases { .. } => {}
            Statement::ShowColumns { .. } => {}
            Statement::ShowVariable { .. } => {}
            Statement::ShowVariables { .. } => {}
            Statement::ShowStatus { .. } => {}
            Statement::ShowCreate { .. } => {}
            Statement::Explain { .. } => {}
            Statement::ExplainTable { .. } => {}
            Statement::Analyze { .. } => {}
            Statement::Set(_) => {} // Needed for SET statements

            // Blocked with specific error messages
            Statement::Insert(_) => {
                anyhow::bail!("k8sql is read-only: INSERT is not supported")
            }
            Statement::Update { .. } => {
                anyhow::bail!("k8sql is read-only: UPDATE is not supported")
            }
            Statement::Delete(_) => {
                anyhow::bail!("k8sql is read-only: DELETE is not supported")
            }
            Statement::CreateTable(_) => {
                anyhow::bail!("k8sql is read-only: CREATE TABLE is not supported")
            }
            Statement::CreateView { .. } => {
                anyhow::bail!("k8sql is read-only: CREATE VIEW is not supported")
            }
            Statement::CreateIndex(_) => {
                anyhow::bail!("k8sql is read-only: CREATE INDEX is not supported")
            }
            Statement::CreateSchema { .. } => {
                anyhow::bail!("k8sql is read-only: CREATE SCHEMA is not supported")
            }
            Statement::CreateDatabase { .. } => {
                anyhow::bail!("k8sql is read-only: CREATE DATABASE is not supported")
            }
            Statement::Drop { .. } => {
                anyhow::bail!("k8sql is read-only: DROP is not supported")
            }
            Statement::AlterTable { .. } => {
                anyhow::bail!("k8sql is read-only: ALTER TABLE is not supported")
            }
            Statement::AlterView { .. } => {
                anyhow::bail!("k8sql is read-only: ALTER VIEW is not supported")
            }
            Statement::AlterIndex { .. } => {
                anyhow::bail!("k8sql is read-only: ALTER INDEX is not supported")
            }
            Statement::Truncate { .. } => {
                anyhow::bail!("k8sql is read-only: TRUNCATE is not supported")
            }
            Statement::Merge { .. } => {
                anyhow::bail!("k8sql is read-only: MERGE is not supported")
            }

            // Catch-all for other unsupported statements
            _ => {
                anyhow::bail!("k8sql is read-only: this statement type is not supported")
            }
        }
    }
    Ok(())
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

    #[test]
    fn test_json_arrow_precedence_in() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' IN ('Running', 'Succeeded')";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') IN ('Running', 'Succeeded')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_not_in() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' NOT IN ('Failed', 'Unknown')";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') NOT IN ('Failed', 'Unknown')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_in_labels() {
        let sql = "SELECT name FROM pods WHERE labels->>'app' IN ('nginx', 'apache')";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (labels->>'app') IN ('nginx', 'apache')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_is_null() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' IS NULL";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') IS NULL"
        );
    }

    #[test]
    fn test_json_arrow_precedence_is_not_null() {
        let sql = "SELECT name FROM pods WHERE labels->>'app' IS NOT NULL";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (labels->>'app') IS NOT NULL"
        );
    }

    #[test]
    fn test_chained_json_arrows_two_levels() {
        let sql = "SELECT spec->'selector'->>'app' FROM services";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT json_get_str(json_get(spec, 'selector'), 'app') FROM services"
        );
    }

    #[test]
    fn test_chained_json_arrows_three_levels() {
        let sql = "SELECT metadata->'labels'->'app'->>'version' FROM pods";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT json_get_str(json_get(json_get(metadata, 'labels'), 'app'), 'version') FROM pods"
        );
    }

    #[test]
    fn test_chained_json_arrows_in_where() {
        let sql = "SELECT * FROM pods WHERE spec->'selector'->>'app' = 'nginx'";
        let result = preprocess_sql(sql);
        assert!(result.contains("json_get_str(json_get(spec, 'selector'), 'app')"));
    }

    #[test]
    fn test_chained_json_arrows_with_table_prefix() {
        let sql = "SELECT p.spec->'containers'->>'name' FROM pods p";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "SELECT json_get_str(json_get(p.spec, 'containers'), 'name') FROM pods p"
        );
    }

    // Tests for validate_read_only

    #[test]
    fn test_read_only_allows_select() {
        assert!(validate_read_only("SELECT * FROM pods").is_ok());
        assert!(
            validate_read_only("SELECT name, namespace FROM pods WHERE namespace = 'default'")
                .is_ok()
        );
    }

    #[test]
    fn test_read_only_allows_select_with_cte() {
        assert!(validate_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte").is_ok());
    }

    #[test]
    fn test_read_only_allows_show_tables() {
        assert!(validate_read_only("SHOW TABLES").is_ok());
    }

    #[test]
    fn test_read_only_allows_show_databases() {
        assert!(validate_read_only("SHOW DATABASES").is_ok());
    }

    #[test]
    fn test_read_only_allows_explain() {
        assert!(validate_read_only("EXPLAIN SELECT * FROM pods").is_ok());
    }

    #[test]
    fn test_read_only_blocks_create_table() {
        let result = validate_read_only("CREATE TABLE test (id INT)");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("CREATE TABLE"));
    }

    #[test]
    fn test_read_only_blocks_insert() {
        let result = validate_read_only("INSERT INTO test VALUES (1)");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("INSERT"));
    }

    #[test]
    fn test_read_only_blocks_update() {
        let result = validate_read_only("UPDATE test SET id = 1");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("UPDATE"));
    }

    #[test]
    fn test_read_only_blocks_delete() {
        let result = validate_read_only("DELETE FROM test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DELETE"));
    }

    #[test]
    fn test_read_only_blocks_drop() {
        let result = validate_read_only("DROP TABLE test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("DROP"));
    }

    #[test]
    fn test_read_only_blocks_alter() {
        let result = validate_read_only("ALTER TABLE test ADD COLUMN name VARCHAR");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("ALTER"));
    }

    #[test]
    fn test_read_only_blocks_truncate() {
        let result = validate_read_only("TRUNCATE TABLE test");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("TRUNCATE"));
    }
}
