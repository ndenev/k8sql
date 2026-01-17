// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Custom query hooks for k8sql

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Set, Statement};
use datafusion_postgres::QueryHook;
use datafusion_postgres::pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response,
};
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};

use crate::kubernetes::K8sClientPool;

/// Hook to handle SET configuration commands from PostgreSQL clients
///
/// Many PostgreSQL clients (DBeaver, pgAdmin, psql) send SET commands
/// on connection for parameters like extra_float_digits, application_name, etc.
/// We accept these to maintain compatibility, logging useful ones.
pub struct SetConfigHook;

impl SetConfigHook {
    pub fn new() -> Self {
        Self
    }

    /// Extract parameter name and value from SET statement
    fn extract_set_params(statement: &Statement) -> Option<(String, String)> {
        match statement {
            Statement::Set(set) => match set {
                Set::SingleAssignment {
                    variable, values, ..
                } => {
                    let name = variable.to_string().to_lowercase();
                    let val = values
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    Some((name, val))
                }
                Set::SetTimeZone { value, .. } => Some(("timezone".to_string(), value.to_string())),
                Set::SetNames { charset_name, .. } => {
                    Some(("client_encoding".to_string(), charset_name.to_string()))
                }
                Set::SetNamesDefault {} => {
                    Some(("client_encoding".to_string(), "DEFAULT".to_string()))
                }
                // Accept other SET variants (SetRole, SetTransaction, etc.)
                _ => Some(("_other".to_string(), set.to_string())),
            },
            _ => None,
        }
    }

    fn handle_set(&self, statement: &Statement) -> Option<PgWireResult<Response>> {
        if let Some((name, value)) = Self::extract_set_params(statement) {
            match name.as_str() {
                "application_name" => {
                    tracing::info!(application_name = %value, "Client application identified");
                }
                "timezone" | "time zone" => {
                    tracing::debug!(timezone = %value, "Timezone requested (k8sql uses UTC internally)");
                }
                "statement_timeout" => {
                    tracing::debug!(timeout = %value, "Statement timeout requested (not enforced)");
                }
                "client_encoding" => {
                    let normalized = value.trim_matches('\'').to_uppercase();
                    if normalized != "UTF8" && normalized != "UTF-8" && normalized != "DEFAULT" {
                        tracing::warn!(
                            encoding = %value,
                            "Non-UTF8 encoding requested (k8sql is UTF-8 only)"
                        );
                    }
                }
                "_other" => {
                    tracing::trace!(statement = %value, "SET statement accepted");
                }
                _ => {
                    tracing::trace!(param = %name, value = %value, "SET parameter accepted");
                }
            }
            return Some(Ok(Response::EmptyQuery));
        }

        None
    }
}

impl Default for SetConfigHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryHook for SetConfigHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.handle_set(statement)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // For SET commands, return a dummy plan
        if Self::extract_set_params(statement).is_some() {
            let schema = Arc::new(Schema::empty());
            let result = schema
                .to_dfschema()
                .map(|df_schema| {
                    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::new(df_schema),
                    })
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)));
            return Some(result);
        }
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.handle_set(statement)
    }
}

/// Hook to handle SHOW TABLES command with clean output
pub struct ShowTablesHook;

impl ShowTablesHook {
    pub fn new() -> Self {
        Self
    }

    fn create_response(&self, session_context: &SessionContext) -> PgWireResult<Response> {
        // Get all table names from the session
        let mut tables: Vec<String> = session_context
            .catalog_names()
            .into_iter()
            .flat_map(|catalog| {
                session_context
                    .catalog(&catalog)
                    .map(|c| {
                        c.schema_names()
                            .into_iter()
                            .flat_map(|schema| {
                                c.schema(&schema)
                                    .map(|s| s.table_names())
                                    .unwrap_or_default()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect();
        tables.sort();

        let fields = vec![FieldInfo::new(
            "table_name".to_string(),
            None,
            None,
            Type::VARCHAR,
            FieldFormat::Text,
        )];
        let fields = Arc::new(fields);

        let mut encoded_rows = Vec::new();
        for table in &tables {
            let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
            encoder.encode_field(&Some(table.as_str()))?;
            encoded_rows.push(Ok(encoder.take_row()));
        }

        let row_stream = futures::stream::iter(encoded_rows);
        Ok(Response::Query(QueryResponse::new(
            fields,
            Box::pin(row_stream),
        )))
    }
}

impl Default for ShowTablesHook {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueryHook for ShowTablesHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::ShowTables { .. } => Some(self.create_response(session_context)),
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match statement {
            Statement::ShowTables { .. } => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "table_name",
                    DataType::Utf8,
                    false,
                )]));
                let result = schema
                    .to_dfschema()
                    .map(|df_schema| {
                        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                            produce_one_row: true,
                            schema: Arc::new(df_schema),
                        })
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)));
                Some(result)
            }
            _ => None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::ShowTables { .. } => Some(self.create_response(session_context)),
            _ => None,
        }
    }
}

/// Hook to handle SHOW DATABASES command by listing Kubernetes contexts
pub struct ShowDatabasesHook {
    pool: Arc<K8sClientPool>,
}

impl ShowDatabasesHook {
    pub fn new(pool: Arc<K8sClientPool>) -> Self {
        Self { pool }
    }

    async fn create_response(&self) -> PgWireResult<Response> {
        let contexts = self.pool.list_contexts().unwrap_or_default();
        let current_contexts = self.pool.current_contexts().await;

        let fields = vec![
            FieldInfo::new(
                "database".to_string(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "selected".to_string(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
        ];
        let fields = Arc::new(fields);

        // Build all rows
        let mut encoded_rows = Vec::new();
        for ctx in &contexts {
            let current_marker = if current_contexts.contains(ctx) {
                "*"
            } else {
                ""
            };
            let mut encoder = DataRowEncoder::new(Arc::clone(&fields));
            encoder.encode_field(&Some(ctx.as_str()))?;
            encoder.encode_field(&Some(current_marker))?;
            encoded_rows.push(Ok(encoder.take_row()));
        }

        let row_stream = futures::stream::iter(encoded_rows);
        Ok(Response::Query(QueryResponse::new(
            fields,
            Box::pin(row_stream),
        )))
    }
}

#[async_trait]
impl QueryHook for ShowDatabasesHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::ShowDatabases { .. } => Some(self.create_response().await),
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match statement {
            Statement::ShowDatabases { .. } => {
                // Return a dummy logical plan for extended query protocol
                let schema = Arc::new(Schema::new(vec![
                    Field::new("database", DataType::Utf8, false),
                    Field::new("selected", DataType::Utf8, false),
                ]));
                let result = schema
                    .to_dfschema()
                    .map(|df_schema| {
                        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                            produce_one_row: true,
                            schema: Arc::new(df_schema),
                        })
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)));
                Some(result)
            }
            _ => None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::ShowDatabases { .. } => Some(self.create_response().await),
            _ => None,
        }
    }
}
