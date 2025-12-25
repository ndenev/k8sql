// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

mod cli;
pub mod config;
mod daemon;
mod datafusion_integration;
mod kubernetes;
mod output;
pub mod progress;
mod sql;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;

use cli::{Args, Command};
use daemon::PgWireServer;
use datafusion_integration::K8sSessionContext;
use kubernetes::K8sClientPool;

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider (aws-lc-rs)
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let args = Args::parse();

    // Initialize logging (only for non-interactive modes to avoid TUI interference)
    if args.verbose && (args.query.is_some() || args.file.is_some()) {
        tracing_subscriber::fmt()
            .with_env_filter("k8sql=debug")
            .init();
    }

    // Handle subcommands
    if let Some(cmd) = &args.command {
        match cmd {
            Command::Interactive => {
                run_interactive(&args).await?;
            }
            Command::Daemon { port, bind } => {
                if args.verbose {
                    tracing_subscriber::fmt()
                        .with_env_filter("k8sql=debug")
                        .init();
                }
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
    let pool = Arc::new(K8sClientPool::new(
        args.context.as_deref(),
        &args.namespace,
    )?);
    pool.initialize().await?;
    let session = K8sSessionContext::new(Arc::clone(&pool)).await?;

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
            let mut tables = session.list_tables();
            tables.sort();
            let result = output::QueryResult {
                columns: vec!["table_name".to_string()],
                rows: tables.into_iter().map(|t| vec![t]).collect(),
            };
            println!("{}", result.format(&args.output, args.no_headers));
            continue;
        }

        // Handle SHOW DATABASES specially
        if normalized == "SHOW DATABASES" {
            let contexts = pool.list_contexts().unwrap_or_default();
            let current_contexts = pool.current_contexts().await;
            let result = output::QueryResult {
                columns: vec!["database".to_string(), "selected".to_string()],
                rows: contexts
                    .iter()
                    .map(|ctx| {
                        vec![
                            ctx.clone(),
                            if current_contexts.contains(ctx) {
                                "*".to_string()
                            } else {
                                String::new()
                            },
                        ]
                    })
                    .collect(),
            };
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
