use super::{FormatOptions, OutputFormatter, QueryResult};

pub struct JsonFormatter;

impl OutputFormatter for JsonFormatter {
    fn format(result: &QueryResult, _options: &FormatOptions) -> String {
        let rows = result.to_json_rows();
        serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
    }
}
