// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use crate::kubernetes::K8sClientPool;
use crate::sql::{QueryExecutor, SqlParser};

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
        let pool = Arc::new(K8sClientPool::new(None, "default").await?);
        let parser = Arc::new(SqlParser::new());
        let executor = Arc::new(QueryExecutor::new(pool));

        let factory = Arc::new(K8sBackendFactory {
            handler: Arc::new(K8sBackend { executor, parser }),
        });

        let server_addr = format!("{}:{}", self.bind_address, self.port);
        let listener = TcpListener::bind(&server_addr).await?;

        tracing::info!(
            "PostgreSQL wire protocol server listening on {}",
            server_addr
        );
        println!("k8sql daemon listening on {}", server_addr);
        println!(
            "Connect with: psql -h {} -p {}",
            self.bind_address, self.port
        );

        loop {
            let incoming_socket = listener.accept().await?;
            let factory_ref = factory.clone();
            let peer_addr = incoming_socket.1;

            tracing::debug!("New connection from {}", peer_addr);

            tokio::spawn(async move {
                if let Err(e) = process_socket(incoming_socket.0, None, factory_ref).await {
                    tracing::error!("Connection error from {}: {}", peer_addr, e);
                }
            });
        }
    }
}

/// Backend handler that executes k8sql queries
struct K8sBackend {
    executor: Arc<QueryExecutor>,
    parser: Arc<SqlParser>,
}

// Implement the NoopStartupHandler trait for trust auth
impl NoopStartupHandler for K8sBackend {}

#[async_trait]
impl SimpleQueryHandler for K8sBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        tracing::debug!("Executing query: {}", query);

        // Parse the query
        let parsed = match self.parser.parse(query) {
            Ok(p) => p,
            Err(e) => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(), // syntax_error
                    format!("Parse error: {}", e),
                ))));
            }
        };

        // Execute the query
        let result = match self.executor.execute(parsed).await {
            Ok(r) => r,
            Err(e) => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(), // internal_error
                    format!("Execution error: {}", e),
                ))));
            }
        };

        // Check if this was a command (USE, SHOW, DESCRIBE) vs a query (SELECT)
        if result.rows.is_empty() && result.columns.len() == 1 && result.columns[0] == "result" {
            // This is likely a command result
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // Build schema from result columns
        let field_infos: Vec<FieldInfo> = result
            .columns
            .iter()
            .map(|name| FieldInfo::new(name.clone(), None, None, Type::TEXT, FieldFormat::Text))
            .collect();

        let schema = Arc::new(field_infos);

        // Encode rows as a stream
        let schema_ref = schema.clone();
        let data_row_stream = stream::iter(result.rows.into_iter()).map(move |row| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for value in row {
                encoder.encode_field(&Some(value))?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            data_row_stream,
        ))])
    }
}

/// Factory that creates handlers for each connection
struct K8sBackendFactory {
    handler: Arc<K8sBackend>,
}

impl PgWireServerHandlers for K8sBackendFactory {
    type StartupHandler = K8sBackend;
    type SimpleQueryHandler = K8sBackend;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}
