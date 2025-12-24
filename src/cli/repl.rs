// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use anyhow::Result;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use console::{style, Style};
use indicatif::{ProgressBar, ProgressStyle};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Editor, Helper};
use std::borrow::Cow;
use std::time::Instant;

use crate::output::QueryResult;
use crate::sql::{QueryExecutor, SqlParser};

// SQL keywords and table names for completion
const TABLES: &[&str] = &[
    "pods", "services", "deployments", "configmaps", "secrets",
    "nodes", "namespaces", "ingresses", "jobs", "cronjobs",
    "statefulsets", "daemonsets", "pvcs", "pvs",
];

const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "ORDER", "BY", "LIMIT", "AND", "OR",
    "SHOW", "TABLES", "DATABASES", "DESCRIBE", "USE", "ASC", "DESC",
    "IN", "LIKE", "NOT", "NULL", "TRUE", "FALSE",
];

const COLUMNS: &[&str] = &[
    "_cluster", "name", "namespace", "status", "age", "labels",
    "status.phase", "spec.replicas", "spec.nodeName",
];

struct SqlHelper;

impl Helper for SqlHelper {}

impl Hinter for SqlHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Validator for SqlHelper {
    fn validate(&self, _ctx: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        Ok(ValidationResult::Valid(None))
    }
}

impl Completer for SqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let line_to_cursor = &line[..pos];

        // Find the start of the current word
        let word_start = line_to_cursor
            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == '=')
            .map(|i| i + 1)
            .unwrap_or(0);

        let prefix = &line_to_cursor[word_start..];
        if prefix.is_empty() {
            return Ok((pos, vec![]));
        }

        let prefix_lower = prefix.to_lowercase();
        let prefix_upper = prefix.to_uppercase();

        let mut matches: Vec<Pair> = Vec::new();

        // Match keywords (case-insensitive, output uppercase)
        for &kw in KEYWORDS {
            if kw.starts_with(&prefix_upper) {
                matches.push(Pair {
                    display: kw.to_string(),
                    replacement: kw.to_string(),
                });
            }
        }

        // Match tables (lowercase)
        for &table in TABLES {
            if table.starts_with(&prefix_lower) {
                matches.push(Pair {
                    display: table.to_string(),
                    replacement: table.to_string(),
                });
            }
        }

        // Match columns (lowercase)
        for &col in COLUMNS {
            if col.starts_with(&prefix_lower) {
                matches.push(Pair {
                    display: col.to_string(),
                    replacement: col.to_string(),
                });
            }
        }

        Ok((word_start, matches))
    }
}

impl Highlighter for SqlHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        // Simple SQL keyword highlighting
        let mut result = line.to_string();

        for &kw in KEYWORDS {
            // Case-insensitive replacement with colored version
            let re = regex::RegexBuilder::new(&format!(r"\b{}\b", regex::escape(kw)))
                .case_insensitive(true)
                .build()
                .unwrap();
            result = re.replace_all(&result, |_caps: &regex::Captures| {
                format!("\x1b[1;34m{}\x1b[0m", kw)
            }).to_string();
        }

        Cow::Owned(result)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(&'s self, prompt: &'p str, _default: bool) -> Cow<'b, str> {
        Cow::Owned(format!("\x1b[1;32m{}\x1b[0m", prompt))
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        true
    }
}

fn create_spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

fn format_table(result: &QueryResult) -> String {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL_CONDENSED);
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Header row with styling
    let header_cells: Vec<Cell> = result
        .columns
        .iter()
        .map(|col| Cell::new(col).fg(Color::Yellow))
        .collect();
    table.set_header(header_cells);

    // Data rows
    for row in &result.rows {
        let cells: Vec<Cell> = row.iter().map(|val| Cell::new(val)).collect();
        table.add_row(cells);
    }

    table.to_string()
}

fn print_welcome() {
    let version = env!("CARGO_PKG_VERSION");
    println!(
        "{} {} - Query Kubernetes with SQL",
        style("k8sql").cyan().bold(),
        style(format!("v{}", version)).dim()
    );
    println!(
        "{}",
        style("Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>").dim()
    );
    println!(
        "{}",
        style("Type 'help' for commands, Ctrl+D to exit").dim()
    );
    println!();
}

fn print_help() {
    let help_style = Style::new().cyan();
    let cmd_style = Style::new().yellow();

    println!("{}", help_style.apply_to("Commands:"));
    println!("  {}  - List available tables (Kubernetes resources)", cmd_style.apply_to("SHOW TABLES"));
    println!("  {}  - List available clusters (kubectl contexts)", cmd_style.apply_to("SHOW DATABASES"));
    println!("  {}  - Show table schema", cmd_style.apply_to("DESCRIBE <table>"));
    println!("  {}  - Switch to a different cluster", cmd_style.apply_to("USE <cluster>"));
    println!();
    println!("{}", help_style.apply_to("Examples:"));
    println!("  {} - All pods in current namespace", cmd_style.apply_to("SELECT * FROM pods"));
    println!("  {} - Filter by namespace", cmd_style.apply_to("SELECT name, status FROM pods WHERE namespace = 'kube-system'"));
    println!("  {} - Query specific cluster", cmd_style.apply_to("SELECT * FROM pods WHERE _cluster = 'prod'"));
    println!("  {} - Query all clusters", cmd_style.apply_to("SELECT * FROM pods WHERE _cluster = '*'"));
    println!();
    println!("{}", help_style.apply_to("Shortcuts:"));
    println!("  {} - SHOW TABLES", cmd_style.apply_to("\\dt"));
    println!("  {} - SHOW DATABASES", cmd_style.apply_to("\\l"));
    println!("  {} - DESCRIBE <table>", cmd_style.apply_to("\\d <table>"));
    println!("  {} - Quit", cmd_style.apply_to("\\q"));
    println!();
}

pub async fn run_repl(executor: QueryExecutor) -> Result<()> {
    let parser = SqlParser::new();

    let helper = SqlHelper;
    let config = rustyline::Config::builder()
        .auto_add_history(true)
        .max_history_size(1000)?
        .build();

    let mut rl: Editor<SqlHelper, DefaultHistory> = Editor::with_config(config)?;
    rl.set_helper(Some(helper));

    // Load history
    let history_path = dirs::home_dir()
        .map(|p| p.join(".k8sql_history"))
        .unwrap_or_else(|| ".k8sql_history".into());
    let _ = rl.load_history(&history_path);

    print_welcome();

    loop {
        let prompt = format!("{}> ", style("k8sql").green().bold());

        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();

                if input.is_empty() {
                    continue;
                }

                // Handle special commands
                let lower = input.to_lowercase();
                if lower == "quit" || lower == "exit" || lower == "\\q" {
                    println!("{}", style("Goodbye!").dim());
                    break;
                }
                if lower == "help" || lower == "\\?" {
                    print_help();
                    continue;
                }
                if lower == "clear" || lower == "\\c" {
                    print!("\x1B[2J\x1B[1;1H"); // Clear screen
                    continue;
                }

                // Execute query with spinner
                let spinner = create_spinner("Executing query...");
                let start = Instant::now();

                match parser.parse(input) {
                    Ok(query) => {
                        match executor.execute(query).await {
                            Ok(result) => {
                                spinner.finish_and_clear();
                                let elapsed = start.elapsed();

                                if result.rows.is_empty() {
                                    println!("{}", style("(0 rows)").dim());
                                } else {
                                    println!("{}", format_table(&result));
                                    println!(
                                        "{}",
                                        style(format!(
                                            "{} row{} ({:.2}s)",
                                            result.rows.len(),
                                            if result.rows.len() == 1 { "" } else { "s" },
                                            elapsed.as_secs_f64()
                                        ))
                                        .dim()
                                    );
                                }
                            }
                            Err(e) => {
                                spinner.finish_and_clear();
                                println!("{} {}", style("Error:").red().bold(), style(e).red());
                            }
                        }
                    }
                    Err(e) => {
                        spinner.finish_and_clear();
                        println!("{} {}", style("Parse error:").red().bold(), style(e).red());
                    }
                }
                println!();
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", style("^C").dim());
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", style("Goodbye!").dim());
                break;
            }
            Err(err) => {
                println!("{} {:?}", style("Error:").red().bold(), err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(&history_path);

    Ok(())
}
