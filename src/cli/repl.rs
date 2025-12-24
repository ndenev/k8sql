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
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::datafusion_integration::K8sSessionContext;
use crate::kubernetes::K8sClientPool;
use crate::output::QueryResult;

// SQL keywords for completion
const KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "ORDER", "BY", "LIMIT", "AND", "OR",
    "SHOW", "TABLES", "DATABASES", "DESCRIBE", "USE", "ASC", "DESC",
    "IN", "LIKE", "NOT", "NULL", "TRUE", "FALSE",
];

// Common operators for WHERE clauses
const OPERATORS: &[&str] = &["=", "!=", "<>", "<", ">", "<=", ">=", "LIKE", "IN", "NOT"];

/// Cached completion data populated from Kubernetes cluster
#[derive(Default)]
pub struct CompletionCache {
    /// All discovered table names
    pub tables: Vec<String>,
    /// Table aliases (e.g., "pod" -> "pods")
    pub aliases: HashMap<String, String>,
    /// Columns per table (table_name -> column names)
    pub columns: HashMap<String, Vec<String>>,
    /// Available namespaces in the cluster
    pub namespaces: Vec<String>,
    /// Available kubectl contexts (clusters)
    pub contexts: Vec<String>,
}

impl CompletionCache {
    /// Populate cache from DataFusion session and K8sClientPool
    pub async fn populate(session: &K8sSessionContext, pool: &K8sClientPool) -> Result<Self> {
        let mut cache = CompletionCache::default();

        // Get contexts from kubeconfig
        cache.contexts = pool.list_contexts()?;

        // Get tables from DataFusion's information_schema
        if let Ok(result) = session
            .execute_sql_to_strings(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'default'",
            )
            .await
        {
            for row in &result.rows {
                if let Some(table_name) = row.first() {
                    cache.tables.push(table_name.clone());
                }
            }
        }

        // Get columns from DataFusion's information_schema
        if let Ok(result) = session
            .execute_sql_to_strings(
                "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'default'",
            )
            .await
        {
            for row in &result.rows {
                if row.len() >= 2 {
                    let table_name = &row[0];
                    let column_name = &row[1];
                    cache
                        .columns
                        .entry(table_name.clone())
                        .or_default()
                        .push(column_name.clone());
                }
            }
        }

        // Fetch namespaces from cluster
        if let Ok(Some(ns_info)) = pool.get_resource_info("namespaces", None).await {
            let client = pool.get_client(None).await?;
            let ar = &ns_info.api_resource;
            let api: kube::Api<kube::api::DynamicObject> = kube::Api::all_with(client, ar);
            if let Ok(ns_list) = api.list(&kube::api::ListParams::default()).await {
                cache.namespaces = ns_list
                    .items
                    .iter()
                    .filter_map(|ns| ns.metadata.name.clone())
                    .collect();
                cache.namespaces.sort();
            }
        }

        cache.tables.sort();
        Ok(cache)
    }

    /// Get columns for a table (resolves aliases)
    fn get_columns(&self, table: &str) -> Option<&Vec<String>> {
        let table_lower = table.to_lowercase();
        let table_name = self.aliases.get(&table_lower).unwrap_or(&table_lower);
        self.columns.get(table_name)
    }
}

struct SqlHelper {
    cache: Arc<RwLock<CompletionCache>>,
}

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

/// Detect completion context from the line before cursor
#[derive(Debug, PartialEq)]
enum CompletionContext {
    /// After FROM or DESCRIBE - complete with table names
    TableName,
    /// After USE - complete with context/cluster names
    ContextName,
    /// After WHERE, AND, OR, or column = - complete with column names
    ColumnName { table: Option<String> },
    /// After namespace = or namespace IN ( - complete with namespace values
    NamespaceValue,
    /// After _cluster = or _cluster IN ( - complete with context values
    ClusterValue,
    /// After a column name - complete with operators (planned feature)
    #[allow(dead_code)]
    Operator,
    /// Default - complete with keywords, tables, columns
    General,
}

impl SqlHelper {
    /// Analyze the line to determine completion context
    fn detect_context(&self, line: &str) -> CompletionContext {
        let line_upper = line.to_uppercase();
        let line_lower = line.to_lowercase();

        // Check for value completion contexts (after = or IN ()
        // Look for patterns like "namespace = '" or "namespace IN ('"
        if line_lower.ends_with("namespace = '")
            || line_lower.ends_with("namespace ='")
            || line_lower.ends_with("namespace in ('")
            || line_lower.ends_with("namespace in('")
            || (line_lower.contains("namespace in (") && line_lower.ends_with("'"))
        {
            return CompletionContext::NamespaceValue;
        }

        if line_lower.ends_with("_cluster = '")
            || line_lower.ends_with("_cluster ='")
            || line_lower.ends_with("_cluster in ('")
            || line_lower.ends_with("_cluster in('")
            || (line_lower.contains("_cluster in (") && line_lower.ends_with("'"))
        {
            return CompletionContext::ClusterValue;
        }

        // Check for USE context
        if line_upper.trim().starts_with("USE ") || line_upper.trim() == "USE" {
            return CompletionContext::ContextName;
        }

        // Extract table name from query for column completion
        let table = self.extract_table_name(&line_lower);

        // Check for FROM or DESCRIBE context
        let words: Vec<&str> = line_upper.split_whitespace().collect();
        if let Some(last_word) = words.last() {
            if *last_word == "FROM" || *last_word == "DESCRIBE" {
                return CompletionContext::TableName;
            }
        }

        // Check if we're in WHERE clause and just typed a column name
        if line_upper.contains("WHERE") || line_upper.contains(" AND ") || line_upper.contains(" OR ") {
            // If the last token looks like a column name (no operator after it)
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                let last_char = trimmed.chars().last().unwrap();
                // If last char is alphanumeric or underscore, might need operator
                if last_char.is_alphanumeric() || last_char == '_' {
                    // Check if there's a space before the current word
                    let parts: Vec<&str> = trimmed.rsplitn(2, char::is_whitespace).collect();
                    if parts.len() == 2 {
                        let last_token = parts[0].to_uppercase();
                        // If last token is not an operator or keyword, suggest operators
                        if !OPERATORS.iter().any(|op| last_token == *op)
                            && !["AND", "OR", "WHERE"].contains(&last_token.as_str())
                        {
                            // Could be column name, suggest operators next
                            // But only if we're definitely after WHERE
                        }
                    }
                }
            }
            return CompletionContext::ColumnName { table };
        }

        // Check for SELECT context (after SELECT, before FROM)
        if line_upper.contains("SELECT") && !line_upper.contains("FROM") {
            return CompletionContext::ColumnName { table };
        }

        CompletionContext::General
    }

    /// Extract table name from a query
    fn extract_table_name(&self, line: &str) -> Option<String> {
        // Look for "FROM table_name" pattern
        let from_idx = line.find("from ")?;
        let after_from = &line[from_idx + 5..];
        let table = after_from
            .split(|c: char| c.is_whitespace() || c == ';')
            .next()?;
        if table.is_empty() {
            None
        } else {
            Some(table.to_string())
        }
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

        // Detect completion context
        let context = self.detect_context(line_to_cursor);

        // Find the start of the current word
        let word_start = line_to_cursor
            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == '=' || c == '\'')
            .map(|i| i + 1)
            .unwrap_or(0);

        let prefix = &line_to_cursor[word_start..];
        let prefix_lower = prefix.to_lowercase();
        let prefix_upper = prefix.to_uppercase();

        let mut matches: Vec<Pair> = Vec::new();

        // Get cache (read lock)
        let cache = self.cache.read().unwrap();

        match context {
            CompletionContext::TableName => {
                // Complete with table names and aliases
                for table in &cache.tables {
                    if table.starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }
                for (alias, _table) in &cache.aliases {
                    if alias.starts_with(&prefix_lower) && !cache.tables.contains(alias) {
                        matches.push(Pair {
                            display: alias.clone(),
                            replacement: alias.clone(),
                        });
                    }
                }
            }

            CompletionContext::ContextName => {
                // Complete with kubectl contexts
                for ctx in &cache.contexts {
                    if ctx.to_lowercase().starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: ctx.clone(),
                            replacement: ctx.clone(),
                        });
                    }
                }
            }

            CompletionContext::NamespaceValue => {
                // Complete with actual namespace names
                for ns in &cache.namespaces {
                    if ns.starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: ns.clone(),
                            replacement: ns.clone(),
                        });
                    }
                }
            }

            CompletionContext::ClusterValue => {
                // Complete with context names (same as ContextName but for WHERE clause)
                for ctx in &cache.contexts {
                    if ctx.to_lowercase().starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: ctx.clone(),
                            replacement: ctx.clone(),
                        });
                    }
                }
                // Add '*' for all clusters
                if "*".starts_with(&prefix_lower) || prefix.is_empty() {
                    matches.push(Pair {
                        display: "*".to_string(),
                        replacement: "*".to_string(),
                    });
                }
            }

            CompletionContext::ColumnName { table } => {
                // If we know the table, use its columns; otherwise use general columns
                let cols = table
                    .as_ref()
                    .and_then(|t| cache.get_columns(t))
                    .or_else(|| cache.columns.get("pods")); // Default to pods columns

                if let Some(columns) = cols {
                    for col in columns {
                        if col.starts_with(&prefix_lower) {
                            matches.push(Pair {
                                display: col.clone(),
                                replacement: col.clone(),
                            });
                        }
                    }
                }

                // Also suggest common JSON path prefixes
                for path in &["metadata.", "spec.", "status.", "labels."] {
                    if path.starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: path.to_string(),
                            replacement: path.to_string(),
                        });
                    }
                }
            }

            CompletionContext::Operator => {
                for &op in OPERATORS {
                    if op.starts_with(&prefix_upper) || op.starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: op.to_string(),
                            replacement: op.to_string(),
                        });
                    }
                }
            }

            CompletionContext::General => {
                // Keywords (uppercase)
                for &kw in KEYWORDS {
                    if kw.starts_with(&prefix_upper) {
                        matches.push(Pair {
                            display: kw.to_string(),
                            replacement: kw.to_string(),
                        });
                    }
                }

                // Tables (lowercase)
                for table in &cache.tables {
                    if table.starts_with(&prefix_lower) {
                        matches.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }

                // Default column set (from pods as common case)
                if let Some(columns) = cache.columns.get("pods") {
                    for col in columns {
                        if col.starts_with(&prefix_lower) {
                            matches.push(Pair {
                                display: col.clone(),
                                replacement: col.clone(),
                            });
                        }
                    }
                }
            }
        }

        // If no prefix yet, don't show completions (avoid noise)
        if prefix.is_empty() && matches.len() > 20 {
            return Ok((pos, vec![]));
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
    println!("  {} - Toggle expanded display", cmd_style.apply_to("\\x"));
    println!("  {} - Quit", cmd_style.apply_to("\\q"));
    println!();
}

fn format_expanded(result: &QueryResult) -> String {
    let mut output = String::new();
    let max_col_width = result.columns.iter().map(|c| c.len()).max().unwrap_or(0);

    for (row_idx, row) in result.rows.iter().enumerate() {
        output.push_str(&format!(
            "{}\n",
            style(format!("-[ RECORD {} ]-", row_idx + 1)).yellow()
        ));
        for (col_idx, value) in row.iter().enumerate() {
            let col_name = &result.columns[col_idx];
            output.push_str(&format!(
                "{:>width$} | {}\n",
                style(col_name).cyan(),
                value,
                width = max_col_width
            ));
        }
    }
    output
}

pub async fn run_repl(mut session: K8sSessionContext, pool: Arc<K8sClientPool>) -> Result<()> {
    // Populate completion cache from DataFusion session
    let cache = CompletionCache::populate(&session, &pool)
        .await
        .unwrap_or_default();
    let cache = Arc::new(RwLock::new(cache));

    let helper = SqlHelper {
        cache: Arc::clone(&cache),
    };
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

    // Display mode: false = table, true = expanded
    let mut expanded = false;

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
                if lower == "\\x" {
                    expanded = !expanded;
                    println!(
                        "Expanded display is {}.",
                        if expanded { "on" } else { "off" }
                    );
                    continue;
                }

                // Check if this is a USE command (for cache refresh after success)
                let is_use_command = lower.starts_with("use ");

                // Handle USE command specially (requires context switch)
                if is_use_command {
                    let context_name = input.trim()[4..].trim();
                    if let Err(e) = pool.switch_context(context_name).await {
                        println!("{} {}", style("Error:").red().bold(), style(e).red());
                    } else {
                        println!("Switched to context: {}", style(context_name).cyan());
                        // Refresh tables after context switch
                        if let Err(e) = session.refresh_tables().await {
                            println!("{} {}", style("Warning:").yellow(), style(e).yellow());
                        }
                        // Refresh completion cache from updated session
                        if let Ok(new_cache) = CompletionCache::populate(&session, &pool).await {
                            if let Ok(mut cache_guard) = cache.write() {
                                *cache_guard = new_cache;
                            }
                        }
                    }
                    println!();
                    continue;
                }

                // Handle SHOW DATABASES specially (list Kubernetes contexts)
                if lower.trim().trim_end_matches(';') == "show databases" {
                    let contexts = pool.list_contexts().unwrap_or_default();
                    let current = pool.current_context().await.unwrap_or_default();
                    let result = QueryResult {
                        columns: vec!["database".to_string(), "current".to_string()],
                        rows: contexts
                            .iter()
                            .map(|ctx| {
                                vec![
                                    ctx.clone(),
                                    if ctx == &current { "*".to_string() } else { String::new() },
                                ]
                            })
                            .collect(),
                    };
                    if expanded {
                        print!("{}", format_expanded(&result));
                    } else {
                        println!("{}", format_table(&result));
                    }
                    println!(
                        "{}",
                        style(format!("{} row{}", result.rows.len(), if result.rows.len() == 1 { "" } else { "s" })).dim()
                    );
                    println!();
                    continue;
                }

                // Execute query with spinner using DataFusion
                let spinner = create_spinner("Executing query...");
                let start = Instant::now();

                match session.execute_sql_to_strings(input).await {
                    Ok(result) => {
                        spinner.finish_and_clear();
                        let elapsed = start.elapsed();

                        if result.rows.is_empty() {
                            println!("{}", style("(0 rows)").dim());
                        } else {
                            if expanded {
                                print!("{}", format_expanded(&result));
                            } else {
                                println!("{}", format_table(&result));
                            }
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
