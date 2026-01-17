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
    /// Examples: -c prod, -c 'prod,staging', -c 'prod-*', -c '*'
    /// Note: Use quotes for patterns with wildcards or commas.
    #[arg(short, long, value_name = "CONTEXT")]
    pub context: Option<String>,

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

    /// Force refresh of CRD schema cache
    /// By default, CRD schemas are cached indefinitely since they rarely change.
    /// Use this flag to force a fresh discovery of all CRD schemas.
    #[arg(long)]
    pub refresh_crds: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_output_format() {
        let args = Args::parse_from(["k8sql"]);
        assert!(matches!(args.output, OutputFormat::Table));
    }

    #[test]
    fn test_output_format_json() {
        let args = Args::parse_from(["k8sql", "-o", "json"]);
        assert!(matches!(args.output, OutputFormat::Json));
    }

    #[test]
    fn test_output_format_csv() {
        let args = Args::parse_from(["k8sql", "-o", "csv"]);
        assert!(matches!(args.output, OutputFormat::Csv));
    }

    #[test]
    fn test_output_format_yaml() {
        let args = Args::parse_from(["k8sql", "-o", "yaml"]);
        assert!(matches!(args.output, OutputFormat::Yaml));
    }

    #[test]
    fn test_context_single() {
        let args = Args::parse_from(["k8sql", "-c", "prod"]);
        assert_eq!(args.context, Some("prod".to_string()));
    }

    #[test]
    fn test_context_comma_separated() {
        let args = Args::parse_from(["k8sql", "-c", "prod,staging,dev"]);
        assert_eq!(args.context, Some("prod,staging,dev".to_string()));
    }

    #[test]
    fn test_context_glob_pattern() {
        let args = Args::parse_from(["k8sql", "-c", "prod-*"]);
        assert_eq!(args.context, Some("prod-*".to_string()));
    }

    #[test]
    fn test_context_wildcard() {
        let args = Args::parse_from(["k8sql", "-c", "*"]);
        assert_eq!(args.context, Some("*".to_string()));
    }

    #[test]
    fn test_query_simple() {
        let args = Args::parse_from(["k8sql", "-q", "SELECT * FROM pods"]);
        assert_eq!(args.query, Some("SELECT * FROM pods".to_string()));
    }

    #[test]
    fn test_query_with_special_characters() {
        let args = Args::parse_from([
            "k8sql",
            "-q",
            "SELECT * FROM pods WHERE labels->>'app' = 'nginx'",
        ]);
        assert!(args.query.as_ref().unwrap().contains("->>"));
    }

    #[test]
    fn test_file_flag() {
        let args = Args::parse_from(["k8sql", "-f", "/path/to/query.sql"]);
        assert_eq!(args.file, Some("/path/to/query.sql".to_string()));
    }

    #[test]
    fn test_no_headers_flag() {
        let args = Args::parse_from(["k8sql", "--no-headers"]);
        assert!(args.no_headers);
    }

    #[test]
    fn test_verbose_flag() {
        let args = Args::parse_from(["k8sql", "-v"]);
        assert!(args.verbose);
    }

    #[test]
    fn test_refresh_crds_flag() {
        let args = Args::parse_from(["k8sql", "--refresh-crds"]);
        assert!(args.refresh_crds);
    }

    #[test]
    fn test_daemon_subcommand_defaults() {
        let args = Args::parse_from(["k8sql", "daemon"]);
        match args.command {
            Some(Command::Daemon { port, bind }) => {
                assert_eq!(port, 15432);
                assert_eq!(bind, "127.0.0.1");
            }
            _ => panic!("Expected Daemon command"),
        }
    }

    #[test]
    fn test_daemon_custom_port() {
        let args = Args::parse_from(["k8sql", "daemon", "--port", "5432"]);
        match args.command {
            Some(Command::Daemon { port, .. }) => assert_eq!(port, 5432),
            _ => panic!("Expected Daemon command"),
        }
    }

    #[test]
    fn test_daemon_custom_bind() {
        let args = Args::parse_from(["k8sql", "daemon", "--bind", "0.0.0.0"]);
        match args.command {
            Some(Command::Daemon { bind, .. }) => assert_eq!(bind, "0.0.0.0"),
            _ => panic!("Expected Daemon command"),
        }
    }

    #[test]
    fn test_daemon_both_options() {
        let args = Args::parse_from(["k8sql", "daemon", "-p", "5433", "-b", "192.168.1.1"]);
        match args.command {
            Some(Command::Daemon { port, bind }) => {
                assert_eq!(port, 5433);
                assert_eq!(bind, "192.168.1.1");
            }
            _ => panic!("Expected Daemon command"),
        }
    }

    #[test]
    fn test_interactive_subcommand() {
        let args = Args::parse_from(["k8sql", "interactive"]);
        assert!(matches!(args.command, Some(Command::Interactive)));
    }

    #[test]
    fn test_combined_flags() {
        let args = Args::parse_from([
            "k8sql",
            "-c",
            "prod",
            "-q",
            "SELECT name FROM pods",
            "-o",
            "json",
            "-v",
        ]);
        assert_eq!(args.context, Some("prod".to_string()));
        assert_eq!(args.query, Some("SELECT name FROM pods".to_string()));
        assert!(matches!(args.output, OutputFormat::Json));
        assert!(args.verbose);
    }

    #[test]
    fn test_no_command_no_query() {
        // Just k8sql with no arguments (interactive mode by default)
        let args = Args::parse_from(["k8sql"]);
        assert!(args.command.is_none());
        assert!(args.query.is_none());
        assert!(args.context.is_none());
    }
}
