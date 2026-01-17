// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! JSON path preprocessor for k8sql.
//!
//! This module provides intuitive JSON path syntax for accessing nested fields
//! in Kubernetes resource JSON columns (spec, status, labels, etc.).
//!
//! # Syntax
//!
//! - `.field` - Object field access: `status.phase`
//! - `[n]` - Array index (0-based): `spec.containers[0].image`
//! - `[]` - Array expansion (UNNEST): `spec.containers[].image`
//!
//! # Examples
//!
//! ```sql
//! -- Simple field access
//! SELECT status.phase FROM pods
//!
//! -- Nested fields
//! SELECT spec.selector.app FROM deployments
//!
//! -- Array indexing
//! SELECT spec.containers[0].image FROM pods
//!
//! -- Array expansion (one row per container)
//! SELECT spec.containers[].image FROM pods
//! ```
//!
//! # Differentiation from SQL Dots
//!
//! Regular SQL dot notation (schema.table, table.column) is preserved.
//! Only paths starting with known JSON columns are converted:
//! - `public.pods` stays as-is (not a JSON column)
//! - `pods.name` stays as-is (not a JSON column)
//! - `spec.containers` is converted (spec is a JSON column)
//!
//! # Implementation
//!
//! Uses DataFusion's SQL tokenizer to properly handle strings and comments,
//! ensuring JSON paths inside string literals are not converted.

use crate::kubernetes::discovery::DEFAULT_JSON_OBJECT_COLUMNS;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::HashSet;

/// Build a set of JSON column names for efficient lookup.
/// Includes default JSON object columns from discovery.rs plus any additional columns.
pub fn build_json_columns_set(additional_columns: &[&str]) -> HashSet<String> {
    let mut columns: HashSet<String> = DEFAULT_JSON_OBJECT_COLUMNS
        .iter()
        .map(|s| s.to_lowercase())
        .collect();
    columns.extend(additional_columns.iter().map(|s| s.to_lowercase()));
    columns
}

/// Check if a word is a known JSON column
fn is_json_column(word: &str, json_columns: &HashSet<String>) -> bool {
    json_columns.contains(&word.to_lowercase())
}

/// Convert a dot-notation path string to PostgreSQL arrow syntax.
///
/// This function parses paths like `status.phase` or `spec.containers[0].image`
/// and converts them to arrow operators like `status->>'phase'` or
/// `spec->'containers'->0->>'image'`.
///
/// # Arguments
///
/// * `path` - A dot-notation path starting with a JSON column (e.g., "status.phase")
/// * `json_columns` - Optional set of JSON column names. If None, uses DEFAULT_JSON_COLUMNS.
///
/// # Returns
///
/// * `Some(arrow_syntax)` if the path starts with a known JSON column and has segments
/// * `None` if the path doesn't start with a JSON column or has no segments
///
/// # Examples
///
/// ```ignore
/// assert_eq!(
///     convert_path_to_arrows("status.phase", None),
///     Some("status->>'phase'".to_string())
/// );
/// assert_eq!(
///     convert_path_to_arrows("spec.containers[0].image", None),
///     Some("spec->'containers'->0->>'image'".to_string())
/// );
/// assert_eq!(
///     convert_path_to_arrows("name", None),  // Not a JSON column
///     None
/// );
/// ```
pub fn convert_path_to_arrows(
    path: &str,
    json_columns: Option<&HashSet<String>>,
) -> Option<String> {
    let default_columns = build_json_columns_set(&[]);
    let json_columns = json_columns.unwrap_or(&default_columns);

    let mut chars = path.chars().peekable();

    // Extract the JSON column name (first identifier)
    let column = consume_identifier(&mut chars)?;
    if !is_json_column(&column, json_columns) {
        return None;
    }

    // Parse path segments: .field, [n], or []
    let segments = parse_path_segments(&mut chars);
    if segments.is_empty() {
        return None;
    }

    let json_path = JsonPath {
        alias: None,
        column,
        segments,
    };

    Some(json_path.to_sql())
}

/// Consume an identifier (alphanumeric + underscore) from the character iterator.
fn consume_identifier(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<String> {
    let mut ident = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' {
            ident.push(c);
            chars.next();
        } else {
            break;
        }
    }
    if ident.is_empty() { None } else { Some(ident) }
}

/// Consume a field name (alphanumeric + underscore + hyphen) from the character iterator.
fn consume_field_name(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut field = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' || c == '-' {
            field.push(c);
            chars.next();
        } else {
            break;
        }
    }
    field
}

/// Parse path segments (.field, [n], []) from the character iterator.
fn parse_path_segments(chars: &mut std::iter::Peekable<std::str::Chars>) -> Vec<PathSegment> {
    let mut segments = Vec::new();

    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                let field = consume_field_name(chars);
                if !field.is_empty() {
                    segments.push(PathSegment::Field(field));
                }
            }
            '[' => {
                chars.next();
                if let Some(segment) = parse_bracket_segment(chars) {
                    segments.push(segment);
                } else {
                    break;
                }
            }
            _ => break,
        }
    }

    segments
}

/// Parse a bracket segment ([n] or []) and consume the closing bracket.
fn parse_bracket_segment(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<PathSegment> {
    let mut index_str = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            index_str.push(c);
            chars.next();
        } else {
            break;
        }
    }

    // Must have closing bracket
    if chars.peek() != Some(&']') {
        return None;
    }
    chars.next();

    if index_str.is_empty() {
        Some(PathSegment::Expand)
    } else {
        index_str.parse::<usize>().ok().map(PathSegment::Index)
    }
}

/// Parsed segment of a JSON path
#[derive(Debug, Clone, PartialEq)]
enum PathSegment {
    /// Object field access: `.field_name`
    Field(String),
    /// Array index: `[0]`, `[1]`, etc.
    Index(usize),
    /// Array expansion: `[]` - produces UNNEST
    Expand,
}

/// Parsed JSON path with optional table alias
#[derive(Debug)]
struct JsonPath {
    /// Table alias if present, e.g., "p" in `p.spec.containers`
    alias: Option<String>,
    /// The JSON column name, e.g., "spec"
    column: String,
    /// Path segments after the column
    segments: Vec<PathSegment>,
}

impl JsonPath {
    /// Convert this JSON path to SQL expression
    fn to_sql(&self) -> String {
        let expand_positions: Vec<usize> = self
            .segments
            .iter()
            .enumerate()
            .filter_map(|(i, s)| matches!(s, PathSegment::Expand).then_some(i))
            .collect();

        if expand_positions.is_empty() {
            // Simple case: no array expansion
            self.to_arrow_syntax()
        } else {
            // Complex case: array expansion with UNNEST
            self.to_unnest_syntax(&expand_positions)
        }
    }

    /// Convert to PostgreSQL arrow syntax (no UNNEST)
    fn to_arrow_syntax(&self) -> String {
        let prefix = self
            .alias
            .as_ref()
            .map(|a| format!("{}.", a))
            .unwrap_or_default();
        let mut result = format!("{}{}", prefix, self.column);

        for (i, segment) in self.segments.iter().enumerate() {
            let is_last = i == self.segments.len() - 1;
            match segment {
                PathSegment::Field(name) => {
                    let op = if is_last { "->>" } else { "->" };
                    result.push_str(&format!("{}'{}'", op, name));
                }
                PathSegment::Index(idx) => {
                    let op = if is_last { "->>" } else { "->" };
                    result.push_str(&format!("{}{}", op, idx));
                }
                PathSegment::Expand => unreachable!("Expand should be handled by to_unnest_syntax"),
            }
        }
        result
    }

    /// Convert to UNNEST syntax for array expansion
    fn to_unnest_syntax(&self, expand_positions: &[usize]) -> String {
        // For single expansion: spec.containers[].image
        // → (UNNEST(json_get_array(spec, 'containers')))->>'image'

        let first_expand = expand_positions[0];
        let prefix = self
            .alias
            .as_ref()
            .map(|a| format!("{}.", a))
            .unwrap_or_default();

        // Build json_get_array arguments: column name + path components
        let column_expr = format!("{}{}", prefix, self.column);
        let mut path_args: Vec<String> = Vec::new();

        for segment in &self.segments[..first_expand] {
            match segment {
                PathSegment::Field(name) => {
                    path_args.push(format!("'{}'", name));
                }
                PathSegment::Index(idx) => {
                    path_args.push(idx.to_string());
                }
                PathSegment::Expand => unreachable!(),
            }
        }

        // Wrap in UNNEST(json_get_array(...))
        // json_get_array returns a List, UNNEST expands it to rows
        let json_get_args = if path_args.is_empty() {
            // Direct array column: just pass the column
            column_expr
        } else {
            // Nested path: json_get_array(column, 'path1', 'path2', ...)
            format!("{}, {}", column_expr, path_args.join(", "))
        };
        let mut result = format!("UNNEST(json_get_array({}))", json_get_args);

        // Add remaining segments after expansion
        let remaining = &self.segments[first_expand + 1..];
        if !remaining.is_empty() {
            result = format!("({})", result);
            for (i, segment) in remaining.iter().enumerate() {
                let is_last = i == remaining.len() - 1;
                match segment {
                    PathSegment::Field(name) => {
                        let op = if is_last { "->>" } else { "->" };
                        result.push_str(&format!("{}'{}'", op, name));
                    }
                    PathSegment::Index(idx) => {
                        let op = if is_last { "->>" } else { "->" };
                        result.push_str(&format!("{}{}", op, idx));
                    }
                    PathSegment::Expand => {
                        // Nested expansion is complex - for now, we don't support it
                        // Users can use s-strings for complex nested expansions
                        tracing::warn!(
                            "Nested array expansion not supported: use s-strings for complex paths"
                        );
                        // Just leave the [] as-is, it will cause an error
                        result.push_str("[]");
                    }
                }
            }
        }

        result
    }
}

/// Preprocess SQL to convert JSON path syntax to arrow operators.
///
/// This function uses DataFusion's SQL tokenizer to properly handle strings
/// and comments, ensuring JSON paths inside string literals are not converted.
///
/// It finds patterns like `spec.containers[0].image` and converts them to
/// PostgreSQL-style arrow operators like `spec->'containers'->0->>'image'`.
///
/// # Arguments
///
/// * `sql` - The SQL string to preprocess
/// * `json_columns` - Optional set of JSON column names. If None, uses DEFAULT_JSON_COLUMNS.
///
/// # Example
///
/// ```ignore
/// // Use default JSON columns
/// let sql = preprocess_json_paths("SELECT spec.replicas FROM pods", None);
///
/// // Include custom CRD JSON columns
/// let columns = build_json_columns_set(&["customSpec", "customStatus"]);
/// let sql = preprocess_json_paths("SELECT customSpec.field FROM mycrds", Some(&columns));
/// ```
pub fn preprocess_json_paths(sql: &str, json_columns: Option<&HashSet<String>>) -> String {
    // Use default columns if none provided
    let default_columns = build_json_columns_set(&[]);
    let json_columns = json_columns.unwrap_or(&default_columns);

    let dialect = GenericDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, sql);

    let tokens = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(_) => return sql.to_string(), // If tokenization fails, return as-is
    };

    // Process tokens and rebuild SQL with JSON paths converted
    let mut result = String::new();
    let mut i = 0;

    while i < tokens.len() {
        let token = &tokens[i];

        // Look for potential JSON path start: identifier that could be alias or JSON column
        if let Token::Word(_) = token {
            // Check if this could be the start of a JSON path
            // We need to look ahead to see if there's a dot followed by more content
            if let Some(path_result) = try_parse_json_path(&tokens, i, json_columns) {
                result.push_str(&path_result.sql);
                i = path_result.end_index;
                continue;
            }
        }

        // Not a JSON path, add token as-is (with proper escaping)
        result.push_str(&token_to_sql(token));
        i += 1;
    }

    result
}

/// Convert a token back to SQL, properly escaping strings.
///
/// The tokenizer unescapes string content (e.g., `'don''t'` becomes `don't`),
/// so we need to re-escape when converting back to SQL.
fn token_to_sql(token: &Token) -> String {
    match token {
        Token::SingleQuotedString(s) => {
            // Re-escape single quotes by doubling them
            format!("'{}'", s.replace('\'', "''"))
        }
        Token::DoubleQuotedString(s) => {
            // Re-escape double quotes
            format!("\"{}\"", s.replace('"', "\"\""))
        }
        _ => token.to_string(),
    }
}

/// Result of parsing a JSON path from tokens
struct JsonPathResult {
    sql: String,
    end_index: usize,
}

/// Try to parse a JSON path starting at the given index.
/// Returns Some(result) if a valid JSON path was found, None otherwise.
fn try_parse_json_path(
    tokens: &[Token],
    start: usize,
    json_columns: &HashSet<String>,
) -> Option<JsonPathResult> {
    let mut i = start;

    // First, collect all parts of a potential qualified identifier: a.b.c[0].d
    let mut parts: Vec<String> = Vec::new();
    let mut segments: Vec<PathSegment> = Vec::new();
    let mut in_json_path = false;
    let mut json_column_index = None;

    // Collect the first word
    let Token::Word(first_word) = &tokens[i] else {
        return None;
    };
    parts.push(first_word.value.clone());
    i += 1;

    // Check if first word is a JSON column
    if is_json_column(&first_word.value, json_columns) {
        in_json_path = true;
        json_column_index = Some(0);
    }

    // Continue collecting parts separated by dots and brackets
    while i < tokens.len() {
        match &tokens[i] {
            // Dot followed by word
            Token::Period => {
                if i + 1 >= tokens.len() {
                    break;
                }
                match &tokens[i + 1] {
                    Token::Word(word) => {
                        if in_json_path {
                            // We're in JSON path mode, this is a field segment
                            segments.push(PathSegment::Field(word.value.clone()));
                        } else if is_json_column(&word.value, json_columns) {
                            // Found a JSON column after alias
                            in_json_path = true;
                            json_column_index = Some(parts.len());
                            parts.push(word.value.clone());
                        } else {
                            // Regular SQL dot (schema.table or table.column)
                            parts.push(word.value.clone());
                        }
                        i += 2;
                    }
                    Token::Number(num, _) => {
                        if in_json_path {
                            // Numeric field access (rare but possible)
                            segments.push(PathSegment::Field(num.clone()));
                            i += 2;
                        } else {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            // Array bracket
            Token::LBracket => {
                if !in_json_path {
                    break;
                }
                // Look for number or empty (expansion)
                if i + 1 >= tokens.len() {
                    break;
                }
                match &tokens[i + 1] {
                    Token::Number(num, _) => {
                        // Array index
                        if i + 2 < tokens.len() && matches!(tokens[i + 2], Token::RBracket) {
                            if let Ok(idx) = num.parse::<usize>() {
                                segments.push(PathSegment::Index(idx));
                                i += 3;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    Token::RBracket => {
                        // Empty brackets - array expansion
                        segments.push(PathSegment::Expand);
                        i += 2;
                    }
                    _ => break,
                }
            }
            _ => break,
        }
    }

    // If we found a JSON path, convert it
    if in_json_path && !segments.is_empty() {
        let json_col_idx = json_column_index.unwrap();
        let alias = if json_col_idx > 0 {
            Some(parts[..json_col_idx].join("."))
        } else {
            None
        };
        let column = parts[json_col_idx].clone();

        let json_path = JsonPath {
            alias,
            column,
            segments,
        };

        return Some(JsonPathResult {
            sql: json_path.to_sql(),
            end_index: i,
        });
    }

    // Not a JSON path, return None to use original tokens
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_field_access() {
        assert_eq!(
            preprocess_json_paths("SELECT status.phase FROM pods", None),
            "SELECT status->>'phase' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT spec.replicas FROM deployments", None),
            "SELECT spec->>'replicas' FROM deployments"
        );
    }

    #[test]
    fn test_nested_field_access() {
        assert_eq!(
            preprocess_json_paths("SELECT spec.selector.app FROM deployments", None),
            "SELECT spec->'selector'->>'app' FROM deployments"
        );

        assert_eq!(
            preprocess_json_paths("SELECT labels.app FROM pods", None),
            "SELECT labels->>'app' FROM pods"
        );
    }

    #[test]
    fn test_with_table_alias() {
        assert_eq!(
            preprocess_json_paths("SELECT p.status.phase FROM pods p", None),
            "SELECT p.status->>'phase' FROM pods p"
        );
    }

    #[test]
    fn test_array_index() {
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[0].image FROM pods", None),
            "SELECT spec->'containers'->0->>'image' FROM pods"
        );
    }

    #[test]
    fn test_multiple_array_indices() {
        assert_eq!(
            preprocess_json_paths(
                "SELECT spec.containers[0].ports[1].containerPort FROM pods",
                None
            ),
            "SELECT spec->'containers'->0->'ports'->1->>'containerPort' FROM pods"
        );
    }

    #[test]
    fn test_array_expansion() {
        let result = preprocess_json_paths("SELECT spec.containers[].image FROM pods", None);
        assert!(result.contains("UNNEST"));
        assert!(result.contains("json_get_array"));
        assert!(result.contains("->>'image'"));
    }

    #[test]
    fn test_array_expansion_exact_output() {
        // Verify the exact output format for array expansion
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[].name FROM pods", None),
            "SELECT (UNNEST(json_get_array(spec, 'containers')))->>'name' FROM pods"
        );

        // Test array expansion at end of path (no field after [])
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[] FROM pods", None),
            "SELECT UNNEST(json_get_array(spec, 'containers')) FROM pods"
        );
    }

    #[test]
    fn test_non_json_columns_unchanged() {
        // Regular SQL dots should not be converted
        assert_eq!(
            preprocess_json_paths("SELECT pods.name FROM pods", None),
            "SELECT pods.name FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT p.name FROM pods p", None),
            "SELECT p.name FROM pods p"
        );

        assert_eq!(
            preprocess_json_paths("SELECT public.pods FROM schema", None),
            "SELECT public.pods FROM schema"
        );
    }

    #[test]
    fn test_already_converted_unchanged() {
        // Arrow operators should not be modified
        assert_eq!(
            preprocess_json_paths("SELECT status->>'phase' FROM pods", None),
            "SELECT status->>'phase' FROM pods"
        );
    }

    #[test]
    fn test_string_literals_unchanged() {
        // Paths inside string literals should not be converted
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE name = 'status.phase'", None),
            "SELECT * FROM pods WHERE name = 'status.phase'"
        );
    }

    #[test]
    fn test_doubled_single_quotes_preserved() {
        // SQL escaped single quotes ('' -> ') should be preserved correctly
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM namespaces WHERE name != 'doesn''t exist'",
                None
            ),
            "SELECT * FROM namespaces WHERE name <> 'doesn''t exist'"
        );
    }

    #[test]
    fn test_where_clause() {
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.phase = 'Running'", None),
            "SELECT * FROM pods WHERE status->>'phase' = 'Running'"
        );

        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE labels.app = 'nginx'", None),
            "SELECT * FROM pods WHERE labels->>'app' = 'nginx'"
        );
    }

    #[test]
    fn test_multiple_json_paths() {
        assert_eq!(
            preprocess_json_paths("SELECT status.phase, spec.replicas FROM pods", None),
            "SELECT status->>'phase', spec->>'replicas' FROM pods"
        );
    }

    #[test]
    fn test_custom_json_columns() {
        // Test with custom CRD JSON columns
        let columns = build_json_columns_set(&["customSpec", "customStatus"]);
        assert_eq!(
            preprocess_json_paths("SELECT customSpec.field FROM mycrds", Some(&columns)),
            "SELECT customSpec->>'field' FROM mycrds"
        );

        // Default columns still work
        assert_eq!(
            preprocess_json_paths("SELECT spec.replicas FROM pods", Some(&columns)),
            "SELECT spec->>'replicas' FROM pods"
        );
    }

    // =========================================================================
    // Comprehensive JSON Path Preprocessing Tests
    // =========================================================================

    #[test]
    fn test_deep_nesting() {
        // 3 levels deep
        assert_eq!(
            preprocess_json_paths(
                "SELECT spec.template.spec.containers FROM deployments",
                None
            ),
            "SELECT spec->'template'->'spec'->>'containers' FROM deployments"
        );

        // 4 levels deep
        assert_eq!(
            preprocess_json_paths(
                "SELECT spec.template.spec.containers[0].image FROM deployments",
                None
            ),
            "SELECT spec->'template'->'spec'->'containers'->0->>'image' FROM deployments"
        );
    }

    #[test]
    fn test_array_expansion_with_nested_path() {
        // spec.template.spec.containers[].name - common deployment query
        assert_eq!(
            preprocess_json_paths(
                "SELECT spec.template.spec.containers[].name FROM deployments",
                None
            ),
            "SELECT (UNNEST(json_get_array(spec, 'template', 'spec', 'containers')))->>'name' FROM deployments"
        );
    }

    #[test]
    fn test_array_index_at_different_positions() {
        // Index at start
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[0].name FROM pods", None),
            "SELECT spec->'containers'->0->>'name' FROM pods"
        );

        // Index in middle
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[0].ports[1].name FROM pods", None),
            "SELECT spec->'containers'->0->'ports'->1->>'name' FROM pods"
        );

        // Index at end
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[2] FROM pods", None),
            "SELECT spec->'containers'->>2 FROM pods"
        );
    }

    #[test]
    fn test_complex_where_clauses() {
        // AND condition
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods WHERE status.phase = 'Running' AND spec.nodeName = 'node-1'",
                None
            ),
            "SELECT * FROM pods WHERE status->>'phase' = 'Running' AND spec->>'nodeName' = 'node-1'"
        );

        // OR condition
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods WHERE status.phase = 'Running' OR status.phase = 'Pending'",
                None
            ),
            "SELECT * FROM pods WHERE status->>'phase' = 'Running' OR status->>'phase' = 'Pending'"
        );

        // Nested conditions
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods WHERE (status.phase = 'Running' AND labels.app = 'web') OR labels.tier = 'frontend'",
                None
            ),
            "SELECT * FROM pods WHERE (status->>'phase' = 'Running' AND labels->>'app' = 'web') OR labels->>'tier' = 'frontend'"
        );
    }

    #[test]
    fn test_comparison_operators() {
        // Not equals (tokenizer normalizes != to <>)
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.phase != 'Failed'", None),
            "SELECT * FROM pods WHERE status->>'phase' <> 'Failed'"
        );

        // Less than (when comparing as text)
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE spec.replicas > 3", None),
            "SELECT * FROM pods WHERE spec->>'replicas' > 3"
        );

        // LIKE
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.phase LIKE 'Run%'", None),
            "SELECT * FROM pods WHERE status->>'phase' LIKE 'Run%'"
        );

        // IS NULL
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.reason IS NULL", None),
            "SELECT * FROM pods WHERE status->>'reason' IS NULL"
        );

        // IS NOT NULL
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.message IS NOT NULL", None),
            "SELECT * FROM pods WHERE status->>'message' IS NOT NULL"
        );
    }

    #[test]
    fn test_in_clause() {
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods WHERE status.phase IN ('Running', 'Pending')",
                None
            ),
            "SELECT * FROM pods WHERE status->>'phase' IN ('Running', 'Pending')"
        );
    }

    #[test]
    fn test_group_by_and_order_by() {
        // GROUP BY
        assert_eq!(
            preprocess_json_paths(
                "SELECT status.phase, COUNT(*) FROM pods GROUP BY status.phase",
                None
            ),
            "SELECT status->>'phase', COUNT(*) FROM pods GROUP BY status->>'phase'"
        );

        // ORDER BY
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods ORDER BY status.phase", None),
            "SELECT * FROM pods ORDER BY status->>'phase'"
        );

        // ORDER BY with direction
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods ORDER BY status.phase DESC", None),
            "SELECT * FROM pods ORDER BY status->>'phase' DESC"
        );

        // Multiple ORDER BY columns
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods ORDER BY status.phase, spec.nodeName DESC",
                None
            ),
            "SELECT * FROM pods ORDER BY status->>'phase', spec->>'nodeName' DESC"
        );
    }

    #[test]
    fn test_select_aliases() {
        // AS alias
        assert_eq!(
            preprocess_json_paths("SELECT status.phase AS pod_phase FROM pods", None),
            "SELECT status->>'phase' AS pod_phase FROM pods"
        );

        // Multiple aliases
        assert_eq!(
            preprocess_json_paths(
                "SELECT status.phase AS phase, spec.nodeName AS node FROM pods",
                None
            ),
            "SELECT status->>'phase' AS phase, spec->>'nodeName' AS node FROM pods"
        );
    }

    #[test]
    fn test_functions_with_json_paths() {
        // LOWER function
        assert_eq!(
            preprocess_json_paths("SELECT LOWER(status.phase) FROM pods", None),
            "SELECT LOWER(status->>'phase') FROM pods"
        );

        // COALESCE
        assert_eq!(
            preprocess_json_paths("SELECT COALESCE(status.message, 'none') FROM pods", None),
            "SELECT COALESCE(status->>'message', 'none') FROM pods"
        );

        // COUNT DISTINCT
        assert_eq!(
            preprocess_json_paths("SELECT COUNT(DISTINCT status.phase) FROM pods", None),
            "SELECT COUNT(DISTINCT status->>'phase') FROM pods"
        );
    }

    #[test]
    fn test_case_expression() {
        assert_eq!(
            preprocess_json_paths(
                "SELECT CASE WHEN status.phase = 'Running' THEN 'OK' ELSE 'NOT OK' END FROM pods",
                None
            ),
            "SELECT CASE WHEN status->>'phase' = 'Running' THEN 'OK' ELSE 'NOT OK' END FROM pods"
        );
    }

    #[test]
    fn test_subquery() {
        assert_eq!(
            preprocess_json_paths(
                "SELECT * FROM pods WHERE namespace IN (SELECT name FROM namespaces WHERE status.phase = 'Active')",
                None
            ),
            "SELECT * FROM pods WHERE namespace IN (SELECT name FROM namespaces WHERE status->>'phase' = 'Active')"
        );
    }

    #[test]
    fn test_join_with_json_paths() {
        // Simple JOIN
        assert_eq!(
            preprocess_json_paths(
                "SELECT p.name, d.spec.replicas FROM pods p JOIN deployments d ON p.labels.app = d.labels.app",
                None
            ),
            "SELECT p.name, d.spec->>'replicas' FROM pods p JOIN deployments d ON p.labels->>'app' = d.labels->>'app'"
        );
    }

    #[test]
    fn test_real_world_kubernetes_queries() {
        // Get container images from pods
        assert_eq!(
            preprocess_json_paths(
                "SELECT name, spec.containers[0].image AS image FROM pods WHERE namespace = 'default'",
                None
            ),
            "SELECT name, spec->'containers'->0->>'image' AS image FROM pods WHERE namespace = 'default'"
        );

        // Check deployment ready replicas
        assert_eq!(
            preprocess_json_paths(
                "SELECT name, spec.replicas AS desired, status.readyReplicas AS ready FROM deployments",
                None
            ),
            "SELECT name, spec->>'replicas' AS desired, status->>'readyReplicas' AS ready FROM deployments"
        );

        // Events with object reference
        assert_eq!(
            preprocess_json_paths(
                "SELECT message, status.involvedObject.name FROM events WHERE status.involvedObject.kind = 'Pod'",
                None
            ),
            "SELECT message, status->'involvedObject'->>'name' FROM events WHERE status->'involvedObject'->>'kind' = 'Pod'"
        );
    }

    #[test]
    fn test_all_known_json_columns() {
        // Test all default JSON columns are recognized
        assert_eq!(
            preprocess_json_paths("SELECT labels.app FROM pods", None),
            "SELECT labels->>'app' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT annotations.key FROM pods", None),
            "SELECT annotations->>'key' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT spec.field FROM pods", None),
            "SELECT spec->>'field' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT status.field FROM pods", None),
            "SELECT status->>'field' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT data.key FROM configmaps", None),
            "SELECT data->>'key' FROM configmaps"
        );

        assert_eq!(
            preprocess_json_paths("SELECT owner_references.name FROM pods", None),
            "SELECT owner_references->>'name' FROM pods"
        );
    }

    #[test]
    fn test_edge_cases() {
        // Empty query
        assert_eq!(preprocess_json_paths("", None), "");

        // Query with no JSON columns
        assert_eq!(
            preprocess_json_paths("SELECT name, namespace FROM pods", None),
            "SELECT name, namespace FROM pods"
        );

        // JSON column alone (no path after it)
        assert_eq!(
            preprocess_json_paths("SELECT spec FROM pods", None),
            "SELECT spec FROM pods"
        );

        // Similar column names (status vs status2)
        assert_eq!(
            preprocess_json_paths("SELECT status.phase, status2 FROM pods", None),
            "SELECT status->>'phase', status2 FROM pods"
        );

        // Column name containing JSON column name as substring
        assert_eq!(
            preprocess_json_paths("SELECT podspec.field FROM custom_table", None),
            "SELECT podspec.field FROM custom_table"
        );
    }

    #[test]
    fn test_case_insensitivity() {
        // JSON column names should be case-insensitive
        assert_eq!(
            preprocess_json_paths("SELECT STATUS.phase FROM pods", None),
            "SELECT STATUS->>'phase' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT Spec.replicas FROM pods", None),
            "SELECT Spec->>'replicas' FROM pods"
        );

        assert_eq!(
            preprocess_json_paths("SELECT LABELS.app FROM pods", None),
            "SELECT LABELS->>'app' FROM pods"
        );
    }

    #[test]
    fn test_special_characters_preserved() {
        // Wildcards and special SQL
        assert_eq!(
            preprocess_json_paths("SELECT * FROM pods WHERE status.phase = 'Running'", None),
            "SELECT * FROM pods WHERE status->>'phase' = 'Running'"
        );

        // Comments preserved
        assert_eq!(
            preprocess_json_paths("SELECT status.phase -- get phase\nFROM pods", None),
            "SELECT status->>'phase' -- get phase\nFROM pods"
        );
    }

    #[test]
    fn test_mixed_arrow_and_dot_notation() {
        // Mix of already-converted and dot notation
        assert_eq!(
            preprocess_json_paths("SELECT status->>'phase', spec.replicas FROM pods", None),
            "SELECT status->>'phase', spec->>'replicas' FROM pods"
        );
    }

    #[test]
    fn test_double_quoted_identifiers_unchanged() {
        // Double-quoted identifiers should not be converted
        assert_eq!(
            preprocess_json_paths("SELECT \"status.phase\" FROM pods", None),
            "SELECT \"status.phase\" FROM pods"
        );
    }

    // =========================================================================
    // Edge Case Tests - Array Expansion and Indices
    // =========================================================================

    #[test]
    fn test_nested_array_expansion_produces_warning() {
        // Nested array expansion spec.containers[].ports[] is not fully supported.
        // It should produce output (possibly with a warning) but not crash.
        // The second [] should be preserved as literal "[]" in the output.
        let result = preprocess_json_paths(
            "SELECT spec.containers[].ports[].containerPort FROM pods",
            None,
        );

        // Should contain UNNEST for first expansion
        assert!(result.contains("UNNEST"));
        assert!(result.contains("json_get_array"));

        // The second [] should be preserved as literal (current behavior)
        // This documents the current limitation
        assert!(result.contains("[]"));
    }

    #[test]
    fn test_array_expansion_after_index() {
        // spec.containers[0].ports[] - index followed by expansion
        // This is supported: get first container, then expand its ports
        let result = preprocess_json_paths(
            "SELECT spec.containers[0].ports[].containerPort FROM pods",
            None,
        );

        // Should contain UNNEST for the expansion
        assert!(result.contains("UNNEST"));
        assert!(result.contains("json_get_array"));
        // Should access containers with index 0 first
        assert!(
            result.contains("'containers', 0, 'ports'")
                || result.contains("'containers', '0', 'ports'")
        );
    }

    #[test]
    fn test_negative_array_index_not_parsed() {
        // Negative indices like [-1] are not valid - should not be parsed as array access.
        // The tokenizer treats "-1" as a minus operator followed by number,
        // so "[" "-" "1" "]" won't match our array index pattern.
        let result = preprocess_json_paths("SELECT spec.containers[-1].image FROM pods", None);

        // Should not produce arrow syntax for containers (the path is broken)
        // The exact behavior depends on tokenization, but it shouldn't crash
        // and shouldn't produce valid array access
        assert!(!result.contains("->-1"));
    }

    #[test]
    fn test_very_large_array_index() {
        // Very large indices should still work (they'll return null at runtime)
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[999999].image FROM pods", None),
            "SELECT spec->'containers'->999999->>'image' FROM pods"
        );

        // Even larger
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[2147483647].name FROM pods", None),
            "SELECT spec->'containers'->2147483647->>'name' FROM pods"
        );
    }

    #[test]
    fn test_array_index_zero() {
        // Zero index is the most common case
        assert_eq!(
            preprocess_json_paths("SELECT spec.containers[0].image FROM pods", None),
            "SELECT spec->'containers'->0->>'image' FROM pods"
        );
    }

    #[test]
    fn test_multiple_expansions_at_same_level() {
        // Two separate array expansions in same query (different columns)
        let result = preprocess_json_paths(
            "SELECT spec.containers[].image, spec.volumes[].name FROM pods",
            None,
        );

        // Both should produce UNNEST
        // Count occurrences of UNNEST
        let unnest_count = result.matches("UNNEST").count();
        assert_eq!(unnest_count, 2);
    }

    #[test]
    fn test_array_expansion_in_where_clause() {
        // Array expansion in WHERE clause (complex but valid use case)
        let result = preprocess_json_paths(
            "SELECT name FROM pods WHERE spec.containers[].image = 'nginx'",
            None,
        );

        assert!(result.contains("UNNEST"));
        assert!(result.contains("json_get_array"));
    }

    #[test]
    fn test_decimal_in_brackets_not_parsed() {
        // Decimals like [0.5] are not valid array indices
        // The tokenizer produces different tokens for "0.5"
        let result = preprocess_json_paths("SELECT spec.containers[0.5].image FROM pods", None);

        // Should not produce a valid array index access
        // (behavior may vary, but it shouldn't crash)
        assert!(!result.contains("->0.5"));
    }

    #[test]
    fn test_empty_brackets_only() {
        // Just empty brackets on a JSON column
        let result = preprocess_json_paths("SELECT spec.containers[] FROM pods", None);

        assert!(result.contains("UNNEST"));
        assert!(result.contains("json_get_array"));
        // No field access after the expansion
        assert!(!result.contains("->>"));
    }

    // =========================================================================
    // Tests for convert_path_to_arrows (used by PRQL preprocessor)
    // =========================================================================

    #[test]
    fn test_convert_path_simple_field() {
        assert_eq!(
            convert_path_to_arrows("status.phase", None),
            Some("status->>'phase'".to_string())
        );
    }

    #[test]
    fn test_convert_path_nested_fields() {
        assert_eq!(
            convert_path_to_arrows("spec.selector.app", None),
            Some("spec->'selector'->>'app'".to_string())
        );
    }

    #[test]
    fn test_convert_path_array_index() {
        assert_eq!(
            convert_path_to_arrows("spec.containers[0].image", None),
            Some("spec->'containers'->0->>'image'".to_string())
        );
    }

    #[test]
    fn test_convert_path_array_expansion() {
        let result = convert_path_to_arrows("spec.containers[].image", None);
        assert!(result.is_some());
        let sql = result.unwrap();
        assert!(sql.contains("UNNEST"));
        assert!(sql.contains("json_get_array"));
    }

    #[test]
    fn test_convert_path_not_json_column() {
        // "name" is not a JSON column
        assert_eq!(convert_path_to_arrows("name.something", None), None);
    }

    #[test]
    fn test_convert_path_json_column_alone() {
        // Just "status" with no path segments
        assert_eq!(convert_path_to_arrows("status", None), None);
    }

    #[test]
    fn test_convert_path_all_json_columns() {
        assert!(convert_path_to_arrows("labels.app", None).is_some());
        assert!(convert_path_to_arrows("annotations.key", None).is_some());
        assert!(convert_path_to_arrows("spec.field", None).is_some());
        assert!(convert_path_to_arrows("status.field", None).is_some());
        assert!(convert_path_to_arrows("data.key", None).is_some());
        assert!(convert_path_to_arrows("owner_references.name", None).is_some());
    }

    #[test]
    fn test_convert_path_deep_nesting() {
        assert_eq!(
            convert_path_to_arrows("spec.template.spec.containers[0].image", None),
            Some("spec->'template'->'spec'->'containers'->0->>'image'".to_string())
        );
    }

    #[test]
    fn test_convert_path_field_with_hyphen() {
        // Kubernetes often has hyphenated field names
        assert_eq!(
            convert_path_to_arrows("labels.app-name", None),
            Some("labels->>'app-name'".to_string())
        );
    }
}
