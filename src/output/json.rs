use super::QueryResult;

pub struct JsonFormatter;

impl JsonFormatter {
    pub fn format(result: &QueryResult) -> String {
        let rows = result.to_json_rows();
        serde_json::to_string_pretty(&rows).unwrap_or_else(|_| "[]".to_string())
    }
}
