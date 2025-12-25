// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Custom query hooks for k8sql

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::QueryHook;
use datafusion_postgres::pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response,
};
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};

use crate::kubernetes::K8sClientPool;

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
            encoded_rows.push(encoder.finish());
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
