// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use anyhow::Result;
use comfy_table::{Cell, Color, ContentArrangement, Table, presets::UTF8_FULL_CONDENSED};
use console::{Style, style};
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
use tokio::sync::broadcast;

use crate::config::{self, Config};
use crate::datafusion_integration::K8sSessionContext;
use crate::kubernetes::K8sClientPool;
use crate::output::{
    MAX_JSON_COLUMN_WIDTH, QueryResult, WIDE_COLUMNS, show_databases_result, show_tables_result,
    truncate_value,
};
use crate::progress::ProgressUpdate;

// SQL keywords for completion
const KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "ORDER",
    "BY",
    "LIMIT",
    "AND",
    "OR",
    "SHOW",
    "TABLES",
    "DATABASES",
    "DESCRIBE",
    "USE",
    "ASC",
    "DESC",
    "IN",
    "LIKE",
    "NOT",
    "NULL",
    "TRUE",
    "FALSE",
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
        // Get contexts from kubeconfig
        let contexts = pool.list_contexts()?;

        let mut cache = CompletionCache {
            contexts,
            ..Default::default()
        };

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
    /// Pre-compiled regexes for keyword highlighting (avoids recompiling on every keystroke)
    keyword_patterns: Vec<(regex::Regex, &'static str)>,
}

impl SqlHelper {
    fn new(cache: Arc<RwLock<CompletionCache>>) -> Self {
        // Pre-compile keyword regexes once at startup
        let keyword_patterns: Vec<_> = KEYWORDS
            .iter()
            .filter_map(|&kw| {
                regex::RegexBuilder::new(&format!(r"\b{}\b", regex::escape(kw)))
                    .case_insensitive(true)
                    .build()
                    .ok()
                    .map(|re| (re, kw))
            })
            .collect();

        Self {
            cache,
            keyword_patterns,
        }
    }
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
    /// After a column name - complete with operators
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
        if let Some(last_word) = words.last()
            && (*last_word == "FROM" || *last_word == "DESCRIBE")
        {
            return CompletionContext::TableName;
        }

        // Check if we're in WHERE clause context
        let in_where_context = line_upper.contains("WHERE")
            || line_upper.contains(" AND ")
            || line_upper.contains(" OR ");

        if in_where_context {
            let trimmed = line.trim();

            // Check if line ends with space after a potential column name
            // This suggests user wants to type an operator next
            if line.ends_with(' ') && !trimmed.is_empty() {
                // Get the last token before the trailing space
                let tokens: Vec<&str> = trimmed.split_whitespace().collect();
                if let Some(last_token) = tokens.last() {
                    let last_upper = last_token.to_uppercase();

                    // If last token is not already an operator or keyword, suggest operators
                    let is_operator = OPERATORS.iter().any(|op| last_upper == *op);
                    let is_keyword = [
                        "AND", "OR", "WHERE", "NOT", "IN", "LIKE", "IS", "NULL", "BETWEEN",
                    ]
                    .contains(&last_upper.as_str());
                    let is_value = last_token.starts_with('\'') || last_token.ends_with('\'');

                    if !is_operator && !is_keyword && !is_value {
                        return CompletionContext::Operator;
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
                for alias in cache.aliases.keys() {
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
        // Use pre-compiled regex patterns for fast keyword highlighting
        let mut result = line.to_string();

        for (re, kw) in &self.keyword_patterns {
            result = re
                .replace_all(&result, |_caps: &regex::Captures| {
                    format!("\x1b[1;34m{}\x1b[0m", kw)
                })
                .to_string();
        }

        Cow::Owned(result)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
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
            .template("{spinner:.cyan} {msg} {elapsed:.dim}")
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

    // Build a set of column indices that should be truncated
    let truncate_cols: std::collections::HashSet<usize> = result
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            if WIDE_COLUMNS.contains(&col.as_str()) {
                Some(idx)
            } else {
                None
            }
        })
        .collect();

    // Header row with styling
    let header_cells: Vec<Cell> = result
        .columns
        .iter()
        .map(|col| Cell::new(col).fg(Color::Yellow))
        .collect();
    table.set_header(header_cells);

    // Data rows with truncation for wide columns
    for row in &result.rows {
        let cells: Vec<Cell> = row
            .iter()
            .enumerate()
            .map(|(idx, val)| {
                if truncate_cols.contains(&idx) {
                    Cell::new(truncate_value(val, MAX_JSON_COLUMN_WIDTH))
                } else {
                    Cell::new(val)
                }
            })
            .collect();
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
    println!(
        "  {}  - List available tables (Kubernetes resources)",
        cmd_style.apply_to("SHOW TABLES")
    );
    println!(
        "  {}  - List available clusters (kubectl contexts)",
        cmd_style.apply_to("SHOW DATABASES")
    );
    println!(
        "  {}  - Show table schema",
        cmd_style.apply_to("DESCRIBE <table>")
    );
    println!(
        "  {}  - Switch to cluster(s): USE prod, USE prod-*, USE c1, c2, c3",
        cmd_style.apply_to("USE <pattern>")
    );
    println!(
        "  {}  - Rediscover CRDs from cluster (updates cache)",
        cmd_style.apply_to("REFRESH TABLES")
    );
    println!();
    println!("{}", help_style.apply_to("Examples:"));
    println!(
        "  {} - All pods in current namespace",
        cmd_style.apply_to("SELECT * FROM pods")
    );
    println!(
        "  {} - Filter by namespace",
        cmd_style.apply_to("SELECT name, status FROM pods WHERE namespace = 'kube-system'")
    );
    println!(
        "  {} - Query specific cluster",
        cmd_style.apply_to("SELECT * FROM pods WHERE _cluster = 'prod'")
    );
    println!(
        "  {} - Query all clusters",
        cmd_style.apply_to("SELECT * FROM pods WHERE _cluster = '*'")
    );
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

    let helper = SqlHelper::new(Arc::clone(&cache));
    let config = rustyline::Config::builder()
        .auto_add_history(true)
        .max_history_size(1000)?
        .history_ignore_dups(true)?
        .tab_stop(4)
        .build();

    let mut rl: Editor<SqlHelper, DefaultHistory> = Editor::with_config(config)?;
    rl.set_helper(Some(helper));

    // Load history from ~/.k8sql/history
    let history_path = config::base_dir()
        .map(|p| p.join("history"))
        .unwrap_or_else(|_| ".k8sql_history".into());
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

                // Handle backslash shortcuts that translate to SQL
                let input = if lower == "\\dt" {
                    "SHOW TABLES".to_string()
                } else if lower == "\\l" {
                    "SHOW DATABASES".to_string()
                } else if lower.starts_with("\\d ") {
                    // \d <table> -> DESCRIBE <table>
                    let table = input[3..].trim();
                    format!("DESCRIBE {}", table)
                } else {
                    input.to_string()
                };
                let lower = input.to_lowercase();

                // Check if this is a USE command (for cache refresh after success)
                let is_use_command = lower.starts_with("use ");

                // Handle USE command specially (requires context switch)
                if is_use_command {
                    let context_spec = input.trim()[4..].trim().trim_end_matches(';');

                    // Show spinner during context switch
                    let spinner = create_spinner("Switching context...");
                    let mut progress_rx = pool.progress().subscribe();

                    // Run switch_context with progress updates
                    // Use force_refresh=true since user explicitly wants to switch (get fresh data)
                    let switch_result = {
                        let pool = Arc::clone(&pool);
                        let context_spec = context_spec.to_string();
                        let mut switch_handle =
                            Box::pin(async move { pool.switch_context(&context_spec, true).await });

                        loop {
                            tokio::select! {
                                biased;
                                progress = progress_rx.recv() => {
                                    match progress {
                                        Ok(ProgressUpdate::Connecting { cluster }) => {
                                            spinner.set_message(format!("Connecting to {}...", cluster));
                                        }
                                        Ok(ProgressUpdate::Discovering { cluster }) => {
                                            spinner.set_message(format!("Discovering resources on {}...", cluster));
                                        }
                                        Ok(ProgressUpdate::DiscoveryComplete { cluster, table_count, .. }) => {
                                            spinner.set_message(format!("{}: {} tables found", cluster, table_count));
                                        }
                                        _ => {}
                                    }
                                }
                                result = &mut switch_handle => {
                                    break result;
                                }
                            }
                        }
                    };

                    spinner.finish_and_clear();

                    if let Err(e) = switch_result {
                        println!("{} {}", style("Error:").red().bold(), style(e).red());
                    } else {
                        let contexts = pool.current_contexts().await;
                        if contexts.len() == 1 {
                            println!("Switched to context: {}", style(&contexts[0]).cyan());
                        } else {
                            println!(
                                "Switched to {} contexts: {}",
                                style(contexts.len()).cyan(),
                                style(contexts.join(", ")).cyan()
                            );
                        }
                        // Refresh tables after context switch
                        if let Err(e) = session.refresh_tables().await {
                            println!("{} {}", style("Warning:").yellow(), style(e).yellow());
                        }
                        // Refresh completion cache from updated session
                        if let Ok(new_cache) = CompletionCache::populate(&session, &pool).await
                            && let Ok(mut cache_guard) = cache.write()
                        {
                            *cache_guard = new_cache;
                        }
                        // Save selected contexts to config for persistence
                        if let Err(e) = Config::load()
                            .unwrap_or_default()
                            .set_selected_contexts(contexts)
                        {
                            println!(
                                "{} Could not save context selection: {}",
                                style("Warning:").yellow(),
                                style(e).yellow()
                            );
                        }
                    }
                    println!();
                    continue;
                }

                // Handle REFRESH TABLES (force rediscovery of CRDs)
                if lower.trim().trim_end_matches(';') == "refresh tables" {
                    let spinner = create_spinner("Refreshing tables (rediscovering CRDs)...");

                    match pool.refresh_tables().await {
                        Ok(count) => {
                            spinner.finish_and_clear();
                            println!("{}", style(format!("Refreshed {} tables", count)).green());
                        }
                        Err(e) => {
                            spinner.finish_and_clear();
                            println!("{}", style(format!("Error: {}", e)).red());
                        }
                    }
                    println!();
                    continue;
                }

                // Handle SHOW TABLES specially (clean output without catalog/schema noise)
                if lower.trim().trim_end_matches(';') == "show tables" {
                    let mut tables = session.list_tables_with_aliases().await;
                    tables.sort_by(|a, b| a.0.cmp(&b.0));
                    let result = show_tables_result(tables);
                    if expanded {
                        print!("{}", format_expanded(&result));
                    } else {
                        println!("{}", format_table(&result));
                    }
                    println!(
                        "{}",
                        style(format!(
                            "{} table{}",
                            result.rows.len(),
                            if result.rows.len() == 1 { "" } else { "s" }
                        ))
                        .dim()
                    );
                    println!();
                    continue;
                }

                // Handle SHOW DATABASES specially (list Kubernetes contexts)
                if lower.trim().trim_end_matches(';') == "show databases" {
                    let contexts = pool.list_contexts().unwrap_or_default();
                    let current_contexts = pool.current_contexts().await;
                    let result = show_databases_result(contexts, &current_contexts);
                    if expanded {
                        print!("{}", format_expanded(&result));
                    } else {
                        println!("{}", format_table(&result));
                    }
                    println!(
                        "{}",
                        style(format!(
                            "{} row{}",
                            result.rows.len(),
                            if result.rows.len() == 1 { "" } else { "s" }
                        ))
                        .dim()
                    );
                    println!();
                    continue;
                }

                // Execute query with spinner and progress updates
                let spinner = create_spinner("Executing query...");
                let start = Instant::now();

                // Subscribe to progress updates
                let mut progress_rx = pool.progress().subscribe();

                // Clone session for the spawned task
                let session_clone = session.clone();
                let input_owned = input.to_string();

                // Spawn query execution
                let mut query_handle = tokio::spawn(async move {
                    session_clone.execute_sql_to_strings(&input_owned).await
                });

                // Listen for progress updates while query runs
                let result = loop {
                    tokio::select! {
                        // Check for progress updates
                        progress = progress_rx.recv() => {
                            match progress {
                                Ok(ProgressUpdate::StartingQuery { table, cluster_count }) => {
                                    if cluster_count > 1 {
                                        spinner.set_message(format!(
                                            "Querying {} across {} clusters...",
                                            table, cluster_count
                                        ));
                                    } else {
                                        spinner.set_message(format!("Querying {}...", table));
                                    }
                                }
                                Ok(ProgressUpdate::ClusterComplete { cluster, rows, elapsed_ms: _ }) => {
                                    let (done, total) = pool.progress().progress();
                                    spinner.set_message(format!(
                                        "[{}/{}] {} ({} rows)",
                                        done, total, cluster, rows
                                    ));
                                }
                                Ok(ProgressUpdate::QueryComplete { total_rows, elapsed_ms: _ }) => {
                                    spinner.set_message(format!("Processing {} rows...", total_rows));
                                }
                                // Connection/discovery events - shouldn't happen during query
                                Ok(ProgressUpdate::Connecting { .. })
                                | Ok(ProgressUpdate::Connected { .. })
                                | Ok(ProgressUpdate::Discovering { .. })
                                | Ok(ProgressUpdate::DiscoveryComplete { .. })
                                | Ok(ProgressUpdate::RegisteringTables { .. }) => {}
                                Err(broadcast::error::RecvError::Closed) => {
                                    // Channel closed, wait for query
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => {
                                    // Missed some updates, continue
                                }
                            }
                        }
                        // Query completed
                        query_result = &mut query_handle => {
                            break query_result;
                        }
                    }
                };

                match result {
                    Ok(Ok(result)) => {
                        if result.rows.is_empty() {
                            spinner.finish_and_clear();
                            println!("{}", style("(0 rows)").dim());
                        } else {
                            // Show formatting progress for large results
                            spinner
                                .set_message(format!("Formatting {} rows...", result.rows.len()));
                            let output = if expanded {
                                format_expanded(&result)
                            } else {
                                format_table(&result)
                            };
                            spinner.finish_and_clear();
                            if expanded {
                                print!("{}", output);
                            } else {
                                println!("{}", output);
                            }
                            let elapsed = start.elapsed(); // Capture AFTER formatting
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
                    Ok(Err(e)) => {
                        spinner.finish_and_clear();
                        println!("{} {}", style("Error:").red().bold(), style(e).red());
                    }
                    Err(e) => {
                        spinner.finish_and_clear();
                        println!(
                            "{} {}",
                            style("Error:").red().bold(),
                            style(format!("Query task panicked: {}", e)).red()
                        );
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
