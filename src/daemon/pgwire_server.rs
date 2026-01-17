// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use std::sync::Arc;

use datafusion_postgres::datafusion_pg_catalog::pg_catalog::context::EmptyContextProvider;
use datafusion_postgres::datafusion_pg_catalog::setup_pg_catalog;
use datafusion_postgres::{QueryHook, ServerOptions, serve_with_hooks};

use crate::datafusion_integration::{
    K8sSessionContext, SetConfigHook, ShowDatabasesHook, ShowTablesHook,
};
use crate::kubernetes::K8sClientPool;

/// PostgreSQL wire protocol server for k8sql
pub struct PgWireServer {
    port: u16,
    bind_address: String,
    context: Option<String>,
}

impl PgWireServer {
    pub fn new(port: u16, bind_address: String, context: Option<String>) -> Self {
        Self {
            port,
            bind_address,
            context,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // Daemon mode: use --context if specified, otherwise kubeconfig default
        // (does not use saved REPL config for predictability)
        let pool = K8sClientPool::with_context_spec(self.context.as_deref()).await?;
        let session = K8sSessionContext::new(Arc::clone(&pool)).await?;

        // Get the underlying DataFusion SessionContext
        let ctx = session.into_session_context();

        // Register PostgreSQL system catalog tables for client compatibility
        // (DBeaver, pgAdmin, etc. query pg_catalog.* for introspection)
        if let Err(e) = setup_pg_catalog(&ctx, "postgres", EmptyContextProvider) {
            tracing::warn!("Failed to setup pg_catalog: {}", e);
        }

        let ctx = Arc::new(ctx);

        // Create custom hooks for k8sql-specific commands
        let hooks: Vec<Arc<dyn QueryHook>> = vec![
            Arc::new(SetConfigHook::new()), // Handle SET commands from PostgreSQL clients
            Arc::new(ShowTablesHook::new()),
            Arc::new(ShowDatabasesHook::new(Arc::clone(&pool))),
        ];

        let server_options = ServerOptions::new()
            .with_host(self.bind_address.clone())
            .with_port(self.port);

        tracing::info!(
            "PostgreSQL wire protocol server listening on {}:{}",
            self.bind_address,
            self.port
        );
        println!(
            "k8sql daemon listening on {}:{}",
            self.bind_address, self.port
        );
        println!(
            "Connect with: psql -h {} -p {} -U postgres",
            self.bind_address, self.port
        );

        // Use datafusion-postgres to serve queries with our custom hooks
        serve_with_hooks(ctx, &server_options, hooks)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}
