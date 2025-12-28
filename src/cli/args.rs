// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(name = "k8sql")]
#[command(author, version, about = "Query Kubernetes resources using SQL")]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Execute a SQL query directly
    #[arg(short, long)]
    pub query: Option<String>,

    /// Kubernetes context(s) to use. Supports comma-separated list and glob patterns.
    /// Examples: -c prod, -c "prod,staging", -c "prod-*"
    #[arg(short, long, value_name = "CONTEXT")]
    pub context: Option<String>,

    /// Default namespace
    #[arg(short, long, default_value = "default")]
    pub namespace: String,

    /// Output format
    #[arg(short, long, value_enum, default_value = "table")]
    pub output: OutputFormat,

    /// Execute queries from a file
    #[arg(short, long)]
    pub file: Option<String>,

    /// Omit column headers in output
    #[arg(long)]
    pub no_headers: bool,

    /// Enable verbose logging
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start interactive REPL mode
    Interactive,

    /// Start daemon mode (PostgreSQL wire protocol server)
    Daemon {
        /// Port to listen on
        #[arg(short, long, default_value = "15432")]
        port: u16,

        /// Address to bind to
        #[arg(short, long, default_value = "127.0.0.1")]
        bind: String,
    },
}

#[derive(ValueEnum, Clone, Debug, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Csv,
    Yaml,
}
