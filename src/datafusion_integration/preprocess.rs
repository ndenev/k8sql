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

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use regex::Regex;
use std::sync::LazyLock;

/// Regex to fix -> and ->> operator precedence when followed by comparison operators.
///
/// Workaround for upstream bug: https://github.com/apache/datafusion-sqlparser-rs/issues/814
/// The parser uses outdated PostgreSQL 7.0 precedence rules where comparison operators
/// (=, IN, IS NULL, etc.) incorrectly bind more tightly than JSON arrow operators.
///
/// Examples of incorrect parsing without this fix:
///   `col->>'key' = 'val'` parsed as `col->>('key' = 'val')`  ← WRONG
///   `col->'field' IN (...)` parsed as `col->('field' IN (...))` ← WRONG
///
/// We wrap the arrow expression in parentheses before parsing:
///   `col->>'key' = 'val'` becomes `(col->>'key') = 'val'` ← CORRECT
///   `col->'field' IN (...)` becomes `(col->'field') IN (...)` ← CORRECT
///
/// Note: This must be done at string level before parsing because the parser
/// gets the precedence wrong during tokenization.
static JSON_ARROW_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?i)((?:\w+\.)?\w+)\s*->>?\s*'([^']+)'\s*(=|!=|<>|<=|>=|<|>|NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s+IN|IN)"#).unwrap()
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
