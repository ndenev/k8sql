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
