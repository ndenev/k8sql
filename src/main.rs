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
use kubernetes::{K8sClientPool, ResourceCache, extract_initial_context, is_multi_or_pattern_spec};

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
                let server = PgWireServer::new(*port, bind.clone(), args.context.clone());
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
    // If --refresh-crds flag is set, clear the CRD schema cache
    if args.refresh_crds
        && let Ok(cache) = ResourceCache::new()
        && let Err(e) = cache.clear_crds()
    {
        eprintln!("Warning: Failed to clear CRD cache: {}", e);
    }

    // Batch mode: use --context if specified, otherwise kubeconfig default
    // (does not use saved REPL config for predictability)
    let pool = K8sClientPool::with_context_spec(args.context.as_deref()).await?;
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

        // Handle SHOW TABLES specially (enhanced output with metadata)
        if normalized == "SHOW TABLES" {
            let mut metadata = session.list_tables_with_metadata().await;
            metadata.sort_by(|a, b| a.table_name.cmp(&b.table_name));
            let result = output::show_tables_result(metadata);
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
    use progress::{ProgressUpdate, create_spinner};

    // If --refresh-crds flag is set, clear the CRD schema cache
    if args.refresh_crds
        && let Ok(cache) = ResourceCache::new()
        && let Err(e) = cache.clear_crds()
    {
        eprintln!("Warning: Failed to clear CRD cache: {}", e);
    }

    // Determine context spec:
    // - If --context specified: use it (temporary override, not persisted)
    // - If no --context: use saved config, or kubeconfig default
    let (context_spec, using_saved) = if let Some(ref ctx) = args.context {
        (Some(ctx.clone()), false)
    } else {
        let saved = config::Config::load()
            .ok()
            .map(|c| c.selected_contexts)
            .filter(|v| !v.is_empty())
            .map(|v| v.join(", "));
        (saved.clone(), saved.is_some())
    };

    let initial_context = extract_initial_context(context_spec.as_deref());

    // Create a spinner for startup with elapsed time
    let spinner = create_spinner("Connecting to Kubernetes...");

    // Create pool and subscribe to progress BEFORE initialization
    let pool = Arc::new(K8sClientPool::new(initial_context)?);
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

    // If multi-context or pattern, switch to all matching contexts
    if is_multi_or_pattern_spec(context_spec.as_deref())
        && let Some(ref spec) = context_spec
    {
        // Create a new spinner for the multi-context switch
        let spinner = create_spinner("Restoring saved contexts...");
        let mut progress_rx = pool.progress().subscribe();

        // Run switch_context with progress updates
        let switch_result = {
            let pool = Arc::clone(&pool);
            let spec = spec.clone();
            let mut switch_handle =
                Box::pin(async move { pool.switch_context(&spec, false).await });

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
            if using_saved {
                // Non-fatal for saved config: warn and continue
                eprintln!("Warning: Could not restore saved contexts: {}", e);
            } else {
                // Fatal for explicit --context
                return Err(e);
            }
        } else {
            // Refresh tables for multi-context
            let _ = session.refresh_tables().await;
        }
    }

    cli::repl::run_repl(session, pool).await
}
