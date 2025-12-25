use std::borrow::Cow;
use std::collections::HashSet;

use comfy_table::{Table, presets::ASCII_BORDERS_ONLY_CONDENSED};

use super::QueryResult;

/// Maximum width for JSON columns (spec, status, labels, annotations, data)
const MAX_JSON_COLUMN_WIDTH: usize = 60;

/// Columns that should have width limits in table mode
const WIDE_COLUMNS: &[&str] = &["spec", "status", "labels", "annotations", "data"];

/// Truncate a string to max_len chars, adding "..." if truncated
fn truncate_value(s: &str, max_len: usize) -> Cow<'_, str> {
    if s.chars().count() <= max_len {
        Cow::Borrowed(s)
    } else {
        let truncated: String = s.chars().take(max_len.saturating_sub(3)).collect();
        Cow::Owned(format!("{}...", truncated))
    }
}

pub struct TableFormatter;

impl TableFormatter {
    pub fn format(result: &QueryResult, no_headers: bool) -> String {
        if result.rows.is_empty() {
            return "(0 rows)".to_string();
        }

        let mut table = Table::new();
        // ASCII_BORDERS_ONLY_CONDENSED is close to psql style
        table.load_preset(ASCII_BORDERS_ONLY_CONDENSED);

        // Build a set of column indices that should be truncated
        let truncate_cols: HashSet<usize> = result
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if WIDE_COLUMNS.contains(&col.as_str()) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();

        if !no_headers {
            table.set_header(&result.columns);
        }

        for row in &result.rows {
            let cells: Vec<Cow<'_, str>> = row
                .iter()
                .enumerate()
                .map(|(idx, val)| {
                    if truncate_cols.contains(&idx) {
                        truncate_value(val, MAX_JSON_COLUMN_WIDTH)
                    } else {
                        Cow::Borrowed(val.as_str())
                    }
                })
                .collect();
            table.add_row(cells);
        }

        let output = table.to_string();
        format!("{}\n({} rows)", output, result.rows.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_value_short() {
        let short = "hello";
        let result = truncate_value(short, 10);
        assert_eq!(result, "hello");
        // Should be borrowed, not owned
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_truncate_value_exact_length() {
        let exact = "1234567890";
        let result = truncate_value(exact, 10);
        assert_eq!(result, "1234567890");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_truncate_value_too_long() {
        let long = "this is a very long string that needs truncation";
        let result = truncate_value(long, 20);
        // Should be 17 chars + "..." = 20 total
        assert!(result.ends_with("..."));
        assert!(result.chars().count() <= 20);
        assert!(matches!(result, Cow::Owned(_)));
    }

    #[test]
    fn test_truncate_value_unicode() {
        // Unicode characters should be counted as single chars, not bytes
        let unicode = "日本語テストです長い文字列";
        let result = truncate_value(unicode, 8);
        assert!(result.chars().count() <= 8);
        assert!(result.ends_with("..."));
    }

    #[test]
    fn test_truncate_spec_column() {
        let result = QueryResult {
            columns: vec!["name".to_string(), "spec".to_string()],
            rows: vec![vec![
                "nginx".to_string(),
                // 80 char spec - should be truncated to 60
                "a".repeat(80),
            ]],
        };

        let output = TableFormatter::format(&result, false);
        // The output should NOT contain the full 80 char string
        assert!(!output.contains(&"a".repeat(80)));
        // Should contain truncated version with ...
        assert!(output.contains("..."));
    }

    #[test]
    fn test_no_truncate_normal_column() {
        let result = QueryResult {
            columns: vec!["name".to_string(), "uid".to_string()],
            rows: vec![vec![
                "nginx".to_string(),
                // 80 char uid - should NOT be truncated (not a WIDE_COLUMN)
                "a".repeat(80),
            ]],
        };

        let output = TableFormatter::format(&result, false);
        // The full string should be present
        assert!(output.contains(&"a".repeat(80)));
    }

    #[test]
    fn test_wide_columns_list() {
        // Verify all expected columns are in WIDE_COLUMNS
        assert!(WIDE_COLUMNS.contains(&"spec"));
        assert!(WIDE_COLUMNS.contains(&"status"));
        assert!(WIDE_COLUMNS.contains(&"labels"));
        assert!(WIDE_COLUMNS.contains(&"annotations"));
        assert!(WIDE_COLUMNS.contains(&"data"));
        // And some that shouldn't be
        assert!(!WIDE_COLUMNS.contains(&"name"));
        assert!(!WIDE_COLUMNS.contains(&"namespace"));
    }
}
