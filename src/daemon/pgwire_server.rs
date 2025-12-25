// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use std::sync::Arc;

use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::{QueryHook, ServerOptions, serve_with_hooks};

use crate::datafusion_integration::{K8sSessionContext, ShowDatabasesHook};
use crate::kubernetes::K8sClientPool;

/// PostgreSQL wire protocol server for k8sql
pub struct PgWireServer {
    port: u16,
    bind_address: String,
}

impl PgWireServer {
    pub fn new(port: u16, bind_address: String) -> Self {
        Self { port, bind_address }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        // Initialize connection to default context
        let pool = Arc::new(K8sClientPool::new(None, "default")?);
        pool.initialize().await?;
        let session = K8sSessionContext::new(Arc::clone(&pool)).await?;

        // Get the underlying DataFusion SessionContext wrapped in Arc
        let ctx = Arc::new(session.into_session_context());

        // Create default auth manager (accepts "postgres" user with empty password)
        let auth_manager = Arc::new(AuthManager::new());

        // Create custom hooks for k8sql-specific commands
        let hooks: Vec<Arc<dyn QueryHook>> =
            vec![Arc::new(ShowDatabasesHook::new(Arc::clone(&pool)))];

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
        serve_with_hooks(ctx, &server_options, auth_manager, hooks)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}
