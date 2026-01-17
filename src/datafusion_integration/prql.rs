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
}
