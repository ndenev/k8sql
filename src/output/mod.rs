mod table;
mod json;
mod csv;
mod yaml;

pub use table::TableFormatter;
pub use json::JsonFormatter;
pub use csv::CsvFormatter;
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

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
