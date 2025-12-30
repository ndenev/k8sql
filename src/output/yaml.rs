use super::{FormatOptions, OutputFormatter, QueryResult};

pub struct YamlFormatter;

impl OutputFormatter for YamlFormatter {
    fn format(result: &QueryResult, _options: &FormatOptions) -> String {
        let rows = result.to_json_rows();
        serde_yaml::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
    }
}
