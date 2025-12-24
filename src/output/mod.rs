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

    #[allow(dead_code)]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
