use super::QueryResult;

pub struct CsvFormatter;

impl CsvFormatter {
    pub fn format(result: &QueryResult, no_headers: bool) -> String {
        let mut lines = Vec::new();

        if !no_headers {
            lines.push(result.columns.join(","));
        }

        for row in &result.rows {
            let escaped: Vec<String> = row
                .iter()
                .map(|val| {
                    if val.contains(',') || val.contains('"') || val.contains('\n') {
                        format!("\"{}\"", val.replace('"', "\"\""))
                    } else {
                        val.clone()
                    }
                })
                .collect();
            lines.push(escaped.join(","));
        }

        lines.join("\n")
    }
}
