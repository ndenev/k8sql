mod csv;
mod json;
mod table;
mod yaml;

pub use csv::CsvFormatter;
pub use json::JsonFormatter;
pub use table::{MAX_JSON_COLUMN_WIDTH, TableFormatter, WIDE_COLUMNS, truncate_value};
pub use yaml::YamlFormatter;

use crate::cli::OutputFormat;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl QueryResult {
    pub fn format(&self, format: &OutputFormat, no_headers: bool) -> String {
        match format {
            OutputFormat::Table => TableFormatter::format(self, no_headers),
            OutputFormat::Json => JsonFormatter::format(self),
            OutputFormat::Csv => CsvFormatter::format(self, no_headers),
            OutputFormat::Yaml => YamlFormatter::format(self),
        }
    }

    /// Convert rows to JSON Value objects (used by JSON and YAML formatters)
    pub(crate) fn to_json_rows(&self) -> Vec<serde_json::Value> {
        self.rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = self
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, val)| (col.clone(), serde_json::Value::String(val.clone())))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect()
    }
}

/// Create a QueryResult for SHOW TABLES output
/// Takes a sorted list of (table_name, aliases) tuples
pub fn show_tables_result(tables: Vec<(String, String)>) -> QueryResult {
    QueryResult {
        columns: vec!["table_name".to_string(), "aliases".to_string()],
        rows: tables
            .into_iter()
            .map(|(name, aliases)| vec![name, aliases])
            .collect(),
    }
}

/// Create a QueryResult for SHOW DATABASES output
/// Takes a list of context names and which ones are currently selected
pub fn show_databases_result(contexts: Vec<String>, current_contexts: &[String]) -> QueryResult {
    QueryResult {
        columns: vec!["database".to_string(), "selected".to_string()],
        rows: contexts
            .into_iter()
            .map(|ctx| {
                let selected = if current_contexts.contains(&ctx) {
                    "*".to_string()
                } else {
                    String::new()
                };
                vec![ctx, selected]
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_result() -> QueryResult {
        QueryResult {
            columns: vec![
                "name".to_string(),
                "namespace".to_string(),
                "phase".to_string(),
            ],
            rows: vec![
                vec![
                    "nginx".to_string(),
                    "default".to_string(),
                    "Running".to_string(),
                ],
                vec![
                    "redis".to_string(),
                    "cache".to_string(),
                    "Pending".to_string(),
                ],
            ],
        }
    }

    fn empty_result() -> QueryResult {
        QueryResult {
            columns: vec!["name".to_string()],
            rows: vec![],
        }
    }

    #[test]
    fn test_query_result_to_json_rows() {
        let result = sample_result();
        let json_rows = result.to_json_rows();

        assert_eq!(json_rows.len(), 2);

        // Verify first row
        let first = &json_rows[0];
        assert_eq!(first["name"], "nginx");
        assert_eq!(first["namespace"], "default");
        assert_eq!(first["phase"], "Running");

        // Verify second row
        let second = &json_rows[1];
        assert_eq!(second["name"], "redis");
        assert_eq!(second["namespace"], "cache");
        assert_eq!(second["phase"], "Pending");
    }

    #[test]
    fn test_query_result_to_json_rows_empty() {
        let result = empty_result();
        let json_rows = result.to_json_rows();
        assert!(json_rows.is_empty());
    }

    #[test]
    fn test_format_table() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Table, false);

        // Should contain headers
        assert!(output.contains("name"));
        assert!(output.contains("namespace"));
        assert!(output.contains("phase"));

        // Should contain data
        assert!(output.contains("nginx"));
        assert!(output.contains("redis"));

        // Should have row count
        assert!(output.contains("(2 rows)"));
    }

    #[test]
    fn test_format_table_no_headers() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Table, true);

        // Should still contain data
        assert!(output.contains("nginx"));
        assert!(output.contains("redis"));
        assert!(output.contains("(2 rows)"));
    }

    #[test]
    fn test_format_table_empty() {
        let result = empty_result();
        let output = result.format(&OutputFormat::Table, false);
        assert_eq!(output, "(0 rows)");
    }

    #[test]
    fn test_format_json() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Json, false);

        // Parse as JSON to verify correctness
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], "nginx");
        assert_eq!(parsed[1]["name"], "redis");
    }

    #[test]
    fn test_format_json_empty() {
        let result = empty_result();
        let output = result.format(&OutputFormat::Json, false);
        assert_eq!(output, "[]");
    }

    #[test]
    fn test_format_csv() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Csv, false);

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 data rows
        assert_eq!(lines[0], "name,namespace,phase");
        assert_eq!(lines[1], "nginx,default,Running");
        assert_eq!(lines[2], "redis,cache,Pending");
    }

    #[test]
    fn test_format_csv_no_headers() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Csv, true);

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2); // no header, just 2 data rows
        assert_eq!(lines[0], "nginx,default,Running");
        assert_eq!(lines[1], "redis,cache,Pending");
    }

    #[test]
    fn test_format_csv_escaping() {
        let result = QueryResult {
            columns: vec!["name".to_string(), "value".to_string()],
            rows: vec![
                vec!["test".to_string(), "has, comma".to_string()],
                vec!["quoted".to_string(), "has \"quotes\"".to_string()],
                vec!["newline".to_string(), "has\nnewline".to_string()],
            ],
        };
        let output = result.format(&OutputFormat::Csv, false);

        let lines: Vec<&str> = output.lines().collect();
        // Comma should be quoted
        assert!(lines[1].contains("\"has, comma\""));
        // Quotes should be escaped (doubled)
        assert!(lines[2].contains("\"has \"\"quotes\"\"\""));
    }

    #[test]
    fn test_format_yaml() {
        let result = sample_result();
        let output = result.format(&OutputFormat::Yaml, false);

        // Parse as YAML to verify correctness
        let parsed: Vec<serde_yaml::Value> = serde_yaml::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn test_format_yaml_empty() {
        let result = empty_result();
        let output = result.format(&OutputFormat::Yaml, false);
        // Empty array in YAML
        assert_eq!(output.trim(), "[]");
    }
}
