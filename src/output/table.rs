use tabled::settings::Style;

use super::QueryResult;

pub struct TableFormatter;

impl TableFormatter {
    pub fn format(result: &QueryResult, no_headers: bool) -> String {
        if result.rows.is_empty() {
            return "(0 rows)".to_string();
        }

        // Build rows with dynamic columns
        let mut builder = tabled::builder::Builder::default();

        if !no_headers {
            builder.push_record(&result.columns);
        }

        for row in &result.rows {
            builder.push_record(row);
        }

        let mut table = builder.build();
        table.with(Style::psql());

        let output = table.to_string();
        format!("{}\n({} rows)", output, result.rows.len())
    }
}
