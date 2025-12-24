// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

mod cli;
mod daemon;
mod kubernetes;
mod output;
mod sql;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;

use cli::{Args, Command};
use daemon::PgWireServer;
use kubernetes::K8sClientPool;
use sql::{QueryExecutor, SqlParser};

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
    let pool = Arc::new(K8sClientPool::new(args.context.as_deref(), &args.namespace).await?);
    let parser = SqlParser::new();
    let executor = QueryExecutor::new(pool);

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
        match parser.parse(&query_str) {
            Ok(query) => match executor.execute(query).await {
                Ok(result) => {
                    println!("{}", result.format(&args.output, args.no_headers));
                }
                Err(e) => {
                    eprintln!("Error executing query: {}", e);
                    std::process::exit(1);
                }
            },
            Err(e) => {
                eprintln!("Error parsing query: {}", e);
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

async fn run_interactive(args: &Args) -> Result<()> {
    let pool = Arc::new(K8sClientPool::new(args.context.as_deref(), &args.namespace).await?);
    let executor = QueryExecutor::new(Arc::clone(&pool));

    cli::repl::run_repl(executor, pool).await
}
