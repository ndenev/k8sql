use super::{FormatOptions, OutputFormatter, QueryResult};

pub struct CsvFormatter;

impl OutputFormatter for CsvFormatter {
    fn format(result: &QueryResult, options: &FormatOptions) -> String {
        let mut lines = Vec::new();

        if !options.no_headers {
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
