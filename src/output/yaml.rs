use super::QueryResult;

pub struct YamlFormatter;

impl YamlFormatter {
    pub fn format(result: &QueryResult) -> String {
        let rows: Vec<serde_json::Value> = result
            .rows
            .iter()
            .map(|row| {
                let obj: serde_json::Map<String, serde_json::Value> = result
                    .columns
                    .iter()
                    .zip(row.iter())
                    .map(|(col, val)| (col.clone(), serde_json::Value::String(val.clone())))
                    .collect();
                serde_json::Value::Object(obj)
            })
            .collect();

        serde_yaml::to_string(&rows).unwrap_or_else(|_| "[]".to_string())
    }
}
