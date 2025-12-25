use super::QueryResult;

pub struct YamlFormatter;

impl YamlFormatter {
    pub fn format(result: &QueryResult) -> String {
        let rows = result.to_json_rows();
        serde_yaml::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
    }
}
