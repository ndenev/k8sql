// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

mod cli;
pub mod config;
mod daemon;
mod datafusion_integration;
mod kubernetes;
mod output;
pub mod progress;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

use cli::{Args, Command};
use daemon::PgWireServer;
use datafusion_integration::K8sSessionContext;
use kubernetes::K8sClientPool;

/// Initialize logging with file output and optional stderr
fn init_logging(verbose: bool, to_stderr: bool) {
    use tracing_rolling_file::{RollingConditionBase, RollingFileAppenderBase};
    use tracing_subscriber::fmt::format::FmtSpan;

    // Create log directory
    let log_dir = config::base_dir()
        .map(|p| p.join("log"))
        .unwrap_or_else(|_| std::path::PathBuf::from("."));

    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        eprintln!("Warning: Could not create log directory: {}", e);
        return;
    }

    // File appender with size-based rotation:
    // - Max 10MB per file
    // - Keep up to 5 files (total max ~50MB)
    // - Also rotate daily
    let log_path = log_dir.join("k8sql.log");
    let condition = RollingConditionBase::new()
        .daily()
        .max_size(10 * 1024 * 1024); // 10MB

    let file_appender = match RollingFileAppenderBase::new(log_path, condition, 5) {
        Ok(appender) => appender,
        Err(e) => {
            eprintln!("Warning: Could not create log file: {}", e);
            return;
        }
    };

    // Use non-blocking writer for better performance
    let (non_blocking, _guard) = file_appender.get_non_blocking_appender();
    // Leak the guard to keep the background writer alive
    std::mem::forget(_guard);

    let filter = if verbose { "k8sql=debug" } else { "k8sql=info" };
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter));

    // File layer (always enabled)
    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_span_events(FmtSpan::NONE);

    if to_stderr && verbose {
        // Both file and stderr output
        let stderr_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_span_events(FmtSpan::NONE);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .with(stderr_layer)
            .init();
    } else {
        // File only
        tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .init();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider (aws-lc-rs)
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let args = Args::parse();

    // Initialize logging
    // - Always log to file (~/.k8sql/log/k8sql.log)
    // - For batch mode with -v, also log to stderr
    let is_batch = args.query.is_some() || args.file.is_some();
    init_logging(args.verbose, is_batch && args.verbose);

    // Handle subcommands
    if let Some(cmd) = &args.command {
        match cmd {
            Command::Interactive => {
                run_interactive(&args).await?;
            }
            Command::Daemon { port, bind } => {
                let server = PgWireServer::new(*port, bind.clone());
                server.run().await?;
            }
        }
        return Ok(());
    }

    // Batch mode: -q or -f
    if args.query.is_some() || args.file.is_some() {
        run_batch(&args).await?;
        return Ok(());
    }

    // Default: interactive TUI mode
    run_interactive(&args).await
}

async fn run_batch(args: &Args) -> Result<()> {
    // Load saved contexts from config (same as REPL does)
    let saved_contexts = config::Config::load()
        .map(|c| c.selected_contexts)
        .unwrap_or_else(|e| {
            tracing::debug!("Could not load saved contexts: {}", e);
            vec![]
        });

    // Determine context spec: CLI arg takes priority, then saved config
    // Note: We need to own the string for the lifetime of this function
    let context_spec: Option<String> = args.context.clone().or_else(|| {
        if saved_contexts.is_empty() {
            None
        } else {
            // Join saved contexts into comma-separated spec
            Some(saved_contexts.join(", "))
        }
    });

    // Check if context spec contains patterns or multiple contexts
    let is_multi_or_pattern = context_spec
        .as_ref()
        .map(|s| s.contains(',') || s.contains('*') || s.contains('?'))
        .unwrap_or(false);

    // Initialize with first real context from spec (or kubeconfig default)
    // If spec contains patterns, use None and let switch_context handle it
    let initial_context = if is_multi_or_pattern {
        // For patterns/multi-context, start with kubeconfig default
        // switch_context() will handle the actual context selection
        None
    } else {
        context_spec.as_deref()
    };

    let pool = Arc::new(K8sClientPool::new(initial_context, &args.namespace)?);
    pool.initialize().await?;

    // If context spec has multiple contexts or patterns, switch to all of them
    if is_multi_or_pattern && let Some(ref spec) = context_spec {
        pool.switch_context(spec, false).await?;
    }

    let mut session = K8sSessionContext::new(Arc::clone(&pool)).await?;

    // Refresh tables if we switched to multiple contexts after initial setup
    if is_multi_or_pattern {
        session.refresh_tables().await?;
    }

    let queries = if let Some(query) = &args.query {
        vec![query.clone()]
    } else if let Some(file) = &args.file {
        let content = std::fs::read_to_string(file)?;
        content
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.trim().starts_with("--"))
            .map(String::from)
            .collect()
    } else {
        return Ok(());
    };

    for query_str in queries {
        let normalized = query_str.trim().to_uppercase();
        let normalized = normalized.trim_end_matches(';');

        // Handle SHOW TABLES specially (clean output without catalog/schema)
        if normalized == "SHOW TABLES" {
            let mut tables = session.list_tables_with_aliases().await;
            tables.sort_by(|a, b| a.0.cmp(&b.0));
            let result = output::show_tables_result(tables);
            println!("{}", result.format(&args.output, args.no_headers));
            continue;
        }

        // Handle SHOW DATABASES specially
        if normalized == "SHOW DATABASES" {
            let contexts = pool.list_contexts().unwrap_or_default();
            let current_contexts = pool.current_contexts().await;
            let result = output::show_databases_result(contexts, &current_contexts);
            println!("{}", result.format(&args.output, args.no_headers));
            continue;
        }

        match session.execute_sql_to_strings(&query_str).await {
            Ok(result) => {
                println!("{}", result.format(&args.output, args.no_headers));
            }
            Err(e) => {
                eprintln!("Error executing query: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

async fn run_interactive(args: &Args) -> Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use progress::ProgressUpdate;

    // Load saved config (for persistent context selection)
    let saved_config = config::Config::load().ok();
    let saved_contexts = saved_config
        .as_ref()
        .map(|c| c.selected_contexts.clone())
        .unwrap_or_default();

    // Create a spinner for startup with elapsed time
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap(),
    );
    spinner.set_message("Connecting to Kubernetes...");
    spinner.enable_steady_tick(std::time::Duration::from_millis(80));

    // Create pool (fast, no I/O) and subscribe to progress BEFORE initialization
    // If CLI specifies context, use that; otherwise use first saved context or default
    let initial_context = args
        .context
        .as_deref()
        .or_else(|| saved_contexts.first().map(|s| s.as_str()));
    let pool = Arc::new(K8sClientPool::new(initial_context, &args.namespace)?);
    let mut progress_rx = pool.progress().subscribe();

    // Initialize pool and create session with progress updates
    let session_result = {
        let pool = Arc::clone(&pool);
        let mut init_handle = Box::pin(async move {
            pool.initialize().await?;
            K8sSessionContext::new(pool).await
        });

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
                        Ok(ProgressUpdate::RegisteringTables { count }) => {
                            spinner.set_message(format!("Registering {} tables...", count));
                        }
                        _ => {}
                    }
                }
                result = &mut init_handle => {
                    break result;
                }
            }
        }
    };

    spinner.finish_and_clear();

    let mut session = session_result?;

    // If CLI didn't specify context and we have saved contexts with multiple entries,
    // switch to all saved contexts now (we only initialized with the first one)
    // Use force_refresh=false to leverage cache for fast startup
    if args.context.is_none() && saved_contexts.len() > 1 {
        let context_spec = saved_contexts.join(", ");
        if let Err(e) = pool.switch_context(&context_spec, false).await {
            // Non-fatal: just warn and continue with initial context
            eprintln!("Warning: Could not restore saved contexts: {}", e);
        } else {
            // Refresh tables for multi-context
            let _ = session.refresh_tables().await;
        }
    }

    cli::repl::run_repl(session, pool).await
}
