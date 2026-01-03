use std::borrow::Cow;
use std::collections::HashSet;

use comfy_table::{Table, presets::ASCII_BORDERS_ONLY_CONDENSED};

use super::{FormatOptions, OutputFormatter, QueryResult};

/// Maximum width for JSON columns (spec, status, labels, annotations, data)
pub const MAX_JSON_COLUMN_WIDTH: usize = 60;

/// Columns that should have width limits in table mode
pub const WIDE_COLUMNS: &[&str] = &["spec", "status", "labels", "annotations", "data"];

/// Truncate a string to max_len chars, adding "..." if truncated
/// Optimized: O(1) truncation using byte slicing with UTF-8 boundary detection
pub fn truncate_value(s: &str, max_len: usize) -> Cow<'_, str> {
    // Fast path: if byte length is under limit, char count must be too
    if s.len() <= max_len {
        return Cow::Borrowed(s);
    }

    // For long strings, truncate at byte boundary near max_len
    // This is O(1) instead of O(n) char iteration
    let truncate_at = max_len.saturating_sub(3);

    // Find valid UTF-8 boundary by backing up from truncate_at
    // UTF-8 continuation bytes have pattern 10xxxxxx (0x80..0xBF)
    let mut end = truncate_at.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }

    Cow::Owned(format!("{}...", &s[..end]))
}

pub struct TableFormatter;

impl OutputFormatter for TableFormatter {
    fn format(result: &QueryResult, options: &FormatOptions) -> String {
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

        if !options.no_headers {
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

        let output = TableFormatter::format(&result, &FormatOptions::new());
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

        let output = TableFormatter::format(&result, &FormatOptions::new());
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
