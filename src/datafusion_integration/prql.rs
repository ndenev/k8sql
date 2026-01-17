//! PRQL (Pipelined Relational Query Language) support for k8sql.
//!
//! This module provides detection and compilation of PRQL queries to SQL,
//! enabling users to write queries using PRQL's pipeline syntax as an
//! alternative to SQL.
//!
//! # Example
//!
//! ```prql
//! from pods
//! filter namespace == "kube-system"
//! select {name, namespace, created}
//! sort created
//! take 10
//! ```

use anyhow::Result;

/// Detect if input looks like PRQL (vs SQL).
///
/// PRQL queries typically start with keywords like `from`, `let`, or a `prql` header.
/// SQL queries start with keywords like `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, etc.
///
/// This function uses a simple heuristic based on the first non-comment keyword.
/// PRQL uses `#` for single-line comments, so we skip those before checking.
pub fn is_prql(input: &str) -> bool {
    // Skip leading whitespace and PRQL comments (lines starting with #)
    let first_code_line = input
        .lines()
        .map(|line| line.trim())
        .find(|line| !line.is_empty() && !line.starts_with('#'));

    let Some(line) = first_code_line else {
        return false; // Empty or all comments
    };

    let lower = line.to_lowercase();

    // PRQL starts with these keywords
    lower.starts_with("from ")
        || lower.starts_with("from\t")
        || lower.starts_with("let ")
        || lower.starts_with("prql ")
        || lower == "from" // Single word on line (multiline PRQL)
}

/// Compile PRQL source to SQL.
///
/// Uses the `prqlc` compiler with the generic SQL dialect, which should
/// be compatible with DataFusion.
///
/// # Errors
///
/// Returns an error if the PRQL source contains syntax errors or
/// cannot be compiled to SQL.
pub fn compile_prql(prql: &str) -> Result<String> {
    use prqlc::{Options, Target};

    let opts = Options::default()
        .with_target(Target::Sql(Some(prqlc::sql::Dialect::Generic)))
        .no_format();

    prqlc::compile(prql, &opts).map_err(|e| {
        // Format the error messages nicely
        let messages: Vec<String> = e.inner.iter().map(|msg| msg.to_string()).collect();
        anyhow::anyhow!("PRQL compilation error:\n{}", messages.join("\n"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_prql_detection() {
        // PRQL queries
        assert!(is_prql("from pods"));
        assert!(is_prql("from pods | take 5"));
        assert!(is_prql("  from pods"));
        assert!(is_prql("FROM pods")); // case insensitive
        assert!(is_prql("let x = 1"));
        assert!(is_prql("prql target:sql.generic"));

        // PRQL with leading comments
        assert!(is_prql("# comment\nfrom pods"));
        assert!(is_prql("# line 1\n# line 2\nfrom pods | take 5"));
        assert!(is_prql("  # indented comment\n  from pods"));
        assert!(is_prql(
            "# Select all pods\n# from the cluster\nfrom pods\nselect {name}"
        ));

        // Multiline PRQL
        assert!(is_prql("from\npods"));

        // SQL queries - should NOT be detected as PRQL
        assert!(!is_prql("SELECT * FROM pods"));
        assert!(!is_prql("select * from pods"));
        assert!(!is_prql("WITH cte AS (SELECT 1)"));
        assert!(!is_prql("SHOW TABLES"));
        assert!(!is_prql("DESCRIBE pods"));
        assert!(!is_prql("EXPLAIN SELECT * FROM pods"));
        assert!(!is_prql("USE my_cluster"));

        // Edge cases
        assert!(!is_prql("")); // Empty
        assert!(!is_prql("   ")); // Whitespace only
        assert!(!is_prql("# just a comment")); // Comment only
        assert!(!is_prql("# comment 1\n# comment 2")); // Multiple comments only
    }

    #[test]
    fn test_compile_simple_prql() {
        let prql = "from albums | select {title, artist_id}";
        let result = compile_prql(prql);
        assert!(result.is_ok(), "Failed to compile: {:?}", result);

        let sql = result.unwrap();
        assert!(sql.to_lowercase().contains("select"));
        assert!(sql.to_lowercase().contains("from"));
        assert!(sql.to_lowercase().contains("albums"));
    }

    #[test]
    fn test_compile_prql_with_filter() {
        let prql = "from pods | filter namespace == 'kube-system' | take 10";
        let result = compile_prql(prql);
        assert!(result.is_ok(), "Failed to compile: {:?}", result);

        let sql = result.unwrap();
        assert!(sql.to_lowercase().contains("where"));
        assert!(sql.to_lowercase().contains("limit"));
    }

    #[test]
    fn test_compile_invalid_prql() {
        let prql = "this is not valid prql syntax !!!";
        let result = compile_prql(prql);
        assert!(result.is_err());
    }

    // =========================================================================
    // Edge Case Tests - PRQL Detection
    // =========================================================================

    #[test]
    fn test_from_with_tab_separator() {
        // Tab instead of space should work
        assert!(is_prql("from\tpods"));
        assert!(is_prql("from\t\tpods")); // Multiple tabs
        assert!(is_prql("from\t pods")); // Tab then space
    }

    #[test]
    fn test_from_alone_on_line() {
        // "from" alone on its own line (multiline PRQL)
        assert!(is_prql("from"));
        assert!(is_prql("from\n"));
        assert!(is_prql("  from\n  pods"));
    }

    #[test]
    fn test_from_not_detected_when_part_of_word() {
        // "from_table" should NOT be detected as PRQL
        // This is correctly handled because we check "from " with space
        assert!(!is_prql("from_table"));
        assert!(!is_prql("fromnow()"));
        assert!(!is_prql("fromage")); // French cheese :)
    }

    #[test]
    fn test_sql_from_keyword_not_prql() {
        // SQL FROM in various positions
        assert!(!is_prql("SELECT * FROM pods"));
        assert!(!is_prql("SELECT name FROM pods WHERE true"));
        assert!(!is_prql("DELETE FROM pods"));
        assert!(!is_prql("INSERT INTO pods SELECT * FROM other"));
    }

    #[test]
    fn test_prql_header_variations() {
        // prql header with target
        assert!(is_prql("prql target:sql.generic\nfrom pods"));
        assert!(is_prql("PRQL target:sql.generic\nfrom pods")); // uppercase
        assert!(is_prql("prql target:sql.postgres\nfrom pods"));

        // prql with multiple options (space after prql)
        assert!(is_prql("prql version:0.9\nfrom pods"));
    }

    #[test]
    fn test_let_keyword() {
        // let statements
        assert!(is_prql("let x = 1"));
        assert!(is_prql("let my_table = from pods"));
        assert!(is_prql("LET x = 1")); // uppercase

        // "let" alone - not detected (needs space after)
        // This documents current behavior
        assert!(!is_prql("letter")); // "let" as prefix
    }

    #[test]
    fn test_whitespace_edge_cases() {
        // Leading whitespace
        assert!(is_prql("   from pods"));
        assert!(is_prql("\t\tfrom pods"));
        assert!(is_prql("\n\nfrom pods"));

        // Mixed whitespace
        assert!(is_prql(" \t \n from pods"));

        // Only whitespace - not PRQL
        assert!(!is_prql("   "));
        assert!(!is_prql("\t\t\t"));
        assert!(!is_prql("\n\n\n"));
    }

    #[test]
    fn test_comments_then_prql() {
        // Multiple comment styles
        assert!(is_prql("# single comment\nfrom pods"));
        assert!(is_prql("# comment 1\n# comment 2\n# comment 3\nfrom pods"));

        // Indented comments
        assert!(is_prql("  # indented\n  from pods"));

        // Comment with PRQL-like content (should still check actual first code line)
        assert!(is_prql("# this is not from pods\nfrom actual_table"));
    }

    #[test]
    fn test_comments_only_not_prql() {
        // Comments without any code
        assert!(!is_prql("# just a comment"));
        assert!(!is_prql("# line 1\n# line 2\n# line 3"));
        assert!(!is_prql("  # indented comment only"));
    }

    #[test]
    fn test_empty_inputs() {
        assert!(!is_prql(""));
        assert!(!is_prql(" "));
        assert!(!is_prql("\n"));
        assert!(!is_prql("\t"));
    }

    #[test]
    fn test_prql_compile_aggregation() {
        // Test compilation of aggregation queries
        let prql = "from pods | group namespace (aggregate {count = count this})";
        let result = compile_prql(prql);
        assert!(result.is_ok(), "Failed to compile: {:?}", result);

        let sql = result.unwrap();
        assert!(sql.to_lowercase().contains("count"));
        assert!(sql.to_lowercase().contains("group by"));
    }

    #[test]
    fn test_prql_compile_sort() {
        // Sort with direction
        let prql = "from pods | sort {-created}";
        let result = compile_prql(prql);
        assert!(result.is_ok(), "Failed to compile: {:?}", result);

        let sql = result.unwrap();
        assert!(sql.to_lowercase().contains("order by"));
        assert!(sql.to_lowercase().contains("desc"));
    }

    #[test]
    fn test_prql_compile_join() {
        // Basic join
        let prql = "from pods | join deployments (==namespace)";
        let result = compile_prql(prql);
        assert!(result.is_ok(), "Failed to compile: {:?}", result);

        let sql = result.unwrap();
        assert!(sql.to_lowercase().contains("join"));
    }
}
