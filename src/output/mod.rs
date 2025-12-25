mod csv;
mod json;
mod table;
mod yaml;

pub use csv::CsvFormatter;
pub use json::JsonFormatter;
pub use table::TableFormatter;
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
