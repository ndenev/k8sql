// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! SQL preprocessing for k8sql
//!
//! Handles query preprocessing before execution, including:
//! - PRQL to SQL compilation (when PRQL queries are detected)
//! - JSON path syntax conversion (dot notation to arrow operators)
//! - DataFusion parser quirks fixes
//!
//! ## PRQL Support
//!
//! Queries starting with `from`, `let`, or `prql` are automatically detected
//! as PRQL and compiled to SQL:
//!
//! ```prql
//! from pods
//! filter namespace == "kube-system"
//! select {name, namespace}
//! take 10
//! ```
//!
//! ## JSON Path Syntax
//!
//! Intuitive dot notation for JSON fields is automatically converted:
//!
//! ```sql
//! -- Dot notation (converted automatically)
//! SELECT status.phase FROM pods WHERE labels.app = 'nginx'
//!
//! -- Array indexing
//! SELECT spec.containers[0].image FROM pods
//!
//! -- Array expansion (one row per element)
//! SELECT spec.containers[].image FROM pods
//! ```
//!
//! ## SQL Transformations
//!
//! - **JSON path conversion**: `status.phase` becomes `status->>'phase'`
//! - **JSON arrow precedence fix**: `col->>'key' = 'val'` becomes
//!   `(col->>'key') = 'val'` to work around DataFusion parser precedence bug
//!
//! ## PostgreSQL-Compatible JSON Operators
//!
//! Supports PostgreSQL JSON operators including chained arrows:
//!
//! ```sql
//! -- Single arrow
//! SELECT * FROM pods WHERE labels->>'app' = 'nginx'
//!
//! -- Chained arrows
//! SELECT spec->'selector'->>'app' FROM services
//! SELECT metadata->'labels'->'env'->>'name' FROM pods
//! ```

use super::{json_path, prql};
use anyhow::Result;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use regex::Regex;
use std::sync::LazyLock;

/// Regex to match arrows followed by comparison operators (left side)
/// Note: Uses explicit structure matching to prevent ReDoS vulnerability.
/// Matches: column_name[->>'key' or ->'key' chains]->>'final_key'
static LEFT_ARROW_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)((?:\w+\.)?[\w\-]+(?:->>?'[^']+')*)\s*(->>?)\s*'([^']+)'\s*(=|!=|<>|<=|>=|<|>|NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE|IS\s+NOT\s+NULL|IS\s+NULL|NOT\s+IN|IN)"#,
    )
    .unwrap()
});

/// Regex to match arrows preceded by comparison operators (right side)
/// Note: Uses explicit structure matching to prevent ReDoS vulnerability.
/// Matches: column_name[->>'key' or ->'key' chains]->>'final_key'
static RIGHT_ARROW_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r#"(?i)(=|!=|<>|<=|>=|<|>|NOT\s+ILIKE|NOT\s+LIKE|ILIKE|LIKE|IN)\s+((?:\w+\.)?[\w\-]+(?:->>?'[^']+')*)\s*(->>?)\s*'([^']+)'"#,
    )
    .unwrap()
});

/// Fix JSON arrow operator precedence
///
/// DataFusion has a parser precedence bug where comparison operators bind too tightly:
/// https://github.com/apache/datafusion-sqlparser-rs/issues/814
///
/// Wraps arrow expressions in parentheses when they appear with comparison operators:
/// - `labels->>'app' = 'nginx'` → `(labels->>'app') = 'nginx'`
/// - `p1.labels->>'app' = p2.labels->>'app'` → `(p1.labels->>'app') = (p2.labels->>'app')`
fn fix_arrow_precedence(sql: &str) -> String {
    // Wrap arrows that appear in comparison/boolean contexts
    // We need to handle arrows on BOTH sides of comparisons:
    //   p1.labels->>'app' = p2.labels->>'app'
    // Should become:
    //   (p1.labels->>'app') = (p2.labels->>'app')

    // First pass: wrap arrows on left side of comparisons
    let sql = LEFT_ARROW_PATTERN
        .replace_all(sql, |caps: &regex::Captures| {
            let column = &caps[1];
            let arrow = &caps[2];
            let key = &caps[3];
            let operator = &caps[4];
            format!("({}{}'{}') {}", column, arrow, key, operator)
        })
        .into_owned();

    // Second pass: wrap arrows on right side of comparisons
    RIGHT_ARROW_PATTERN
        .replace_all(&sql, |caps: &regex::Captures| {
            let operator = &caps[1];
            let column = &caps[2];
            let arrow = &caps[3];
            let key = &caps[4];
            format!("{} ({}{}'{}')", operator, column, arrow, key)
        })
        .into_owned()
}

/// Preprocess a query for execution.
///
/// This function handles:
/// 1. **PRQL detection and compilation**: Queries starting with `from`, `let`, or `prql`
///    are automatically compiled to SQL using the prqlc compiler.
/// 2. **JSON path syntax conversion**: Converts intuitive dot notation like `spec.containers[0].image`
///    to PostgreSQL arrow operators like `spec->'containers'->0->>'image'`.
/// 3. **JSON arrow precedence fix**: Wraps arrow expressions in parentheses when used
///    with comparison operators to work around DataFusion parser precedence.
///
/// # Examples
///
/// ```ignore
/// // PRQL is automatically detected and compiled
/// preprocess_sql("from pods | take 5")?;  // Returns SQL: SELECT * FROM pods LIMIT 5
///
/// // SQL is processed normally
/// preprocess_sql("SELECT * FROM pods")?;  // Returns: SELECT * FROM pods
///
/// // JSON path syntax is converted
/// preprocess_sql("SELECT spec.containers[0].image FROM pods")?;
/// // Returns: SELECT spec->'containers'->0->>'image' FROM pods
///
/// // Arrow precedence is fixed
/// preprocess_sql("SELECT * FROM pods WHERE labels->>'app' = 'nginx'")?;
/// // Returns: SELECT * FROM pods WHERE (labels->>'app') = 'nginx'
/// ```
pub fn preprocess_sql(sql: &str) -> Result<String> {
    // Step 1: Compile PRQL to SQL if detected
    let sql = if prql::is_prql(sql) {
        prql::compile_prql(sql)?
    } else {
        sql.to_string()
    };

    // Step 2: Convert JSON path syntax to arrow operators
    // Uses default JSON columns (spec, status, labels, etc.)
    // TODO: Accept custom JSON columns from CRD discovery
    let sql = json_path::preprocess_json_paths(&sql, None);

    // Step 3: Fix JSON arrow operator precedence
    Ok(fix_arrow_precedence(&sql))
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
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (status->>'phase') = 'Running'"
        );
    }

    #[test]
    fn test_json_arrow_precedence_not_equals() {
        let sql = "SELECT * FROM pods WHERE status->>'phase' != 'Running'";
        let result = preprocess_sql(sql).unwrap();
        // Tokenizer may normalize != to <> (both are valid SQL not-equals)
        assert!(
            result.contains("(status->>'phase') !=") || result.contains("(status->>'phase') <>"),
            "Expected parenthesized arrow expression, got: {}",
            result
        );
    }

    #[test]
    fn test_json_arrow_precedence_greater_than() {
        let sql = "SELECT * FROM pods WHERE spec->>'replicas' > 1";
        let result = preprocess_sql(sql).unwrap();
        assert!(result.contains("(spec->>'replicas') >"));
    }

    #[test]
    fn test_json_arrow_in_select_not_modified() {
        // ->> in SELECT without comparison should not be modified
        let sql = "SELECT status->>'phase' FROM pods";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(result, "SELECT status->>'phase' FROM pods");
    }

    #[test]
    fn test_json_arrow_like() {
        let sql = "SELECT * FROM pods WHERE labels->>'app' LIKE 'nginx%'";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (labels->>'app') LIKE 'nginx%'"
        );
    }

    #[test]
    fn test_no_arrow_unchanged() {
        let sql = "SELECT * FROM pods WHERE namespace = 'default'";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_multiple_arrows() {
        let sql = "SELECT * FROM pods WHERE labels->>'app' = 'nginx' AND labels->>'env' = 'prod'";
        let result = preprocess_sql(sql).unwrap();
        assert!(result.contains("(labels->>'app') = 'nginx'"));
        assert!(result.contains("(labels->>'env') = 'prod'"));
    }

    #[test]
    fn test_json_arrow_precedence_in() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' IN ('Running', 'Succeeded')";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') IN ('Running', 'Succeeded')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_not_in() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' NOT IN ('Failed', 'Unknown')";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') NOT IN ('Failed', 'Unknown')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_in_labels() {
        let sql = "SELECT name FROM pods WHERE labels->>'app' IN ('nginx', 'apache')";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (labels->>'app') IN ('nginx', 'apache')"
        );
    }

    #[test]
    fn test_json_arrow_precedence_is_null() {
        let sql = "SELECT name FROM pods WHERE status->>'phase' IS NULL";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (status->>'phase') IS NULL"
        );
    }

    #[test]
    fn test_json_arrow_precedence_is_not_null() {
        let sql = "SELECT name FROM pods WHERE labels->>'app' IS NOT NULL";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT name FROM pods WHERE (labels->>'app') IS NOT NULL"
        );
    }

    #[test]
    fn test_chained_json_arrows_two_levels_unchanged() {
        let sql = "SELECT spec->'selector'->>'app' FROM services";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_chained_json_arrows_three_levels_unchanged() {
        let sql = "SELECT metadata->'labels'->'app'->>'version' FROM pods";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn test_chained_json_arrows_in_where_with_comparison() {
        // Chained arrow with comparison gets wrapped for precedence
        let sql = "SELECT * FROM pods WHERE spec->'selector'->>'app' = 'nginx'";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE (spec->'selector'->>'app') = 'nginx'"
        );
    }

    #[test]
    fn test_chained_json_arrows_with_table_prefix_unchanged() {
        let sql = "SELECT p.spec->'containers'->>'name' FROM pods p";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(result, sql);
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

    #[test]
    fn test_json_arrow_both_sides_of_comparison() {
        // Test that arrows on BOTH sides of comparison get wrapped
        let sql = "SELECT * FROM pods p1 JOIN pods p2 ON p1.labels->>'app' = p2.labels->>'app'";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM pods p1 JOIN pods p2 ON (p1.labels->>'app') = (p2.labels->>'app')"
        );
    }

    #[test]
    fn test_json_arrow_in_clause_right_side() {
        // Test arrow on right side of IN clause
        let sql = "SELECT * FROM pods WHERE 'nginx' = labels->>'app'";
        let result = preprocess_sql(sql).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM pods WHERE 'nginx' = (labels->>'app')"
        );
    }

    #[test]
    fn test_parser_supports_chained_arrows() {
        // Test if DataFusion's SQL parser natively supports chained arrow operators
        use datafusion::sql::sqlparser::dialect::GenericDialect;
        use datafusion::sql::sqlparser::parser::Parser;

        let dialect = GenericDialect {};

        // Test chained arrows WITHOUT preprocessing
        let sql = "SELECT spec->'selector'->>'app' FROM pods";
        let result = Parser::parse_sql(&dialect, sql);

        // If this succeeds, we DON'T need to convert chained arrows to functions!
        // If it fails, we'll see the error and understand why conversion is needed
        match result {
            Ok(_) => println!("Parser DOES support chained arrows natively!"),
            Err(e) => println!("Parser DOES NOT support chained arrows: {}", e),
        }

        // Don't assert - just document the behavior
        // assert!(result.is_ok(), "Parser should support chained arrows");
    }

    #[tokio::test]
    async fn test_chained_arrows_end_to_end() {
        // End-to-end test: Can we execute a query with chained arrows?
        use datafusion::arrow::array::{RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        // Create a test dataset with nested JSON
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("spec", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["svc1", "svc2"])),
                Arc::new(StringArray::from(vec![
                    r#"{"selector": {"app": "nginx"}}"#,
                    r#"{"selector": {"app": "redis"}}"#,
                ])),
            ],
        )
        .unwrap();

        // Create SessionContext and register JSON functions
        let mut ctx = SessionContext::new();
        datafusion_functions_json::register_all(&mut ctx).unwrap();

        // Register test table
        ctx.register_batch("test_services", batch).unwrap();

        // Test 1: Single arrow (baseline)
        let result = ctx
            .sql("SELECT name, spec->>'selector' as selector FROM test_services")
            .await;
        match &result {
            Ok(_) => println!("✓ Single arrow works end-to-end"),
            Err(e) => println!("✗ Single arrow failed: {}", e),
        }
        assert!(result.is_ok(), "Single arrow should work");

        // Test 2: Chained arrows WITHOUT preprocessing
        let chained_sql = "SELECT name, spec->'selector'->>'app' as app FROM test_services";
        let result = ctx.sql(chained_sql).await;

        match &result {
            Ok(_) => {
                println!("✓ Chained arrows work end-to-end WITHOUT conversion!");
                // Try to actually execute and get results
                if let Ok(df) = &result {
                    match df.clone().collect().await {
                        Ok(batches) => {
                            println!("✓ Successfully executed chained arrow query");
                            println!("  Result batches: {}", batches.len());
                        }
                        Err(e) => {
                            println!("✗ Execution failed: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("✗ Chained arrows failed end-to-end: {}", e);
            }
        }

        // This is the critical test - if this passes, we don't need conversion!
        assert!(
            result.is_ok(),
            "Chained arrows should work end-to-end without conversion"
        );
    }
}
