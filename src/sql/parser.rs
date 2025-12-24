use anyhow::{anyhow, Result};
use sqlparser::ast::{
    self, BinaryOperator, Expr, ObjectName, OrderByKind, SelectItem, SetExpr, Statement, TableFactor,
    Use as AstUse, Value as SqlValue,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::ast::*;

pub struct SqlParser {
    dialect: PostgreSqlDialect,
}

impl SqlParser {
    pub fn new() -> Self {
        Self {
            dialect: PostgreSqlDialect {},
        }
    }

    pub fn parse(&self, sql: &str) -> Result<Query> {
        let sql_trimmed = sql.trim().trim_end_matches(';');

        // Handle special commands
        let upper = sql_trimmed.to_uppercase();
        if upper == "\\DT" || upper == "SHOW TABLES" {
            return Ok(Query::Show(ShowQuery::Tables));
        }
        if upper == "\\L" || upper == "SHOW DATABASES" {
            return Ok(Query::Show(ShowQuery::Databases));
        }
        if upper.starts_with("\\D ") {
            let table_ref = self.parse_table_string(sql_trimmed[3..].trim());
            return Ok(Query::Describe(DescribeQuery { table_ref }));
        }
        if upper.starts_with("DESCRIBE ") {
            let table_ref = self.parse_table_string(sql_trimmed[9..].trim());
            return Ok(Query::Describe(DescribeQuery { table_ref }));
        }

        let statements = Parser::parse_sql(&self.dialect, sql_trimmed)?;
        if statements.is_empty() {
            return Err(anyhow!("No SQL statement found"));
        }
        if statements.len() > 1 {
            return Err(anyhow!("Only single statements are supported"));
        }

        self.convert_statement(&statements[0])
    }

    /// Parse a table string that may include cluster qualifier
    /// Handles: "table", "cluster.table", "\"cluster.with.dots\".table"
    fn parse_table_string(&self, s: &str) -> TableRef {
        // Try to parse as SQL to handle quoted identifiers
        let sql = format!("SELECT * FROM {}", s);
        if let Ok(stmts) = Parser::parse_sql(&self.dialect, &sql) {
            if let Some(Statement::Query(query)) = stmts.first() {
                if let SetExpr::Select(select) = query.body.as_ref() {
                    if let Some(from) = select.from.first() {
                        if let TableFactor::Table { name, .. } = &from.relation {
                            return self.object_name_to_table_ref(name);
                        }
                    }
                }
            }
        }
        // Fallback: treat as simple table name
        TableRef {
            cluster: None,
            table: s.to_string(),
        }
    }

    /// Convert an ObjectName (potentially multi-part) to a TableRef
    /// "pods" -> TableRef { cluster: None, table: "pods" }
    /// "prod"."pods" -> TableRef { cluster: Some("prod"), table: "pods" }
    /// "prod.cluster.io"."pods" -> TableRef { cluster: Some("prod.cluster.io"), table: "pods" }
    fn object_name_to_table_ref(&self, name: &ObjectName) -> TableRef {
        // Extract identifier values from ObjectNamePart enum
        let parts: Vec<&str> = name.0.iter()
            .filter_map(|part| part.as_ident())
            .map(|ident| ident.value.as_str())
            .collect();

        match parts.len() {
            0 => TableRef {
                cluster: None,
                table: String::new(),
            },
            1 => TableRef {
                cluster: None,
                table: parts[0].to_string(),
            },
            _ => {
                // Last part is the table, everything before is the cluster context
                let table = parts.last().unwrap().to_string();
                let cluster = parts[..parts.len() - 1].join(".");
                TableRef {
                    cluster: Some(cluster),
                    table,
                }
            }
        }
    }

    fn convert_statement(&self, stmt: &Statement) -> Result<Query> {
        match stmt {
            Statement::Query(query) => self.convert_query(query),
            Statement::Use(use_stmt) => {
                // Extract database name from Use statement (handles different USE variants)
                let db_name = match use_stmt {
                    AstUse::Catalog(name)
                    | AstUse::Schema(name)
                    | AstUse::Database(name)
                    | AstUse::Warehouse(name)
                    | AstUse::Role(name)
                    | AstUse::Object(name) => name.to_string(),
                    AstUse::SecondaryRoles(_) => return Err(anyhow!("SECONDARY ROLES not supported")),
                    AstUse::Default => return Err(anyhow!("USE DEFAULT not supported")),
                };
                Ok(Query::Use(UseQuery { database: db_name }))
            }
            Statement::ShowTables { .. } => Ok(Query::Show(ShowQuery::Tables)),
            _ => Err(anyhow!("Unsupported statement type: {:?}", stmt)),
        }
    }

    fn convert_query(&self, query: &ast::Query) -> Result<Query> {
        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select,
            _ => return Err(anyhow!("Only SELECT queries are supported")),
        };

        // Extract table reference (potentially with cluster qualifier)
        if select.from.is_empty() {
            return Err(anyhow!("FROM clause is required"));
        }
        let table_ref = match &select.from[0].relation {
            TableFactor::Table { name, .. } => self.object_name_to_table_ref(name),
            _ => return Err(anyhow!("Complex table expressions not supported")),
        };

        // Extract columns
        let columns = self.convert_select_items(&select.projection)?;

        // Extract WHERE clause
        let where_clause = if let Some(selection) = &select.selection {
            Some(self.convert_where(selection)?)
        } else {
            None
        };

        // Extract ORDER BY
        let order_by = if let Some(ref ob) = query.order_by {
            self.convert_order_by(ob)?
        } else {
            Vec::new()
        };

        // Extract LIMIT
        let limit = self.extract_limit_from_query(query)?;

        Ok(Query::Select(SelectQuery {
            columns,
            table_ref,
            where_clause,
            order_by,
            limit,
        }))
    }

    fn convert_select_items(&self, items: &[SelectItem]) -> Result<Vec<ColumnRef>> {
        let mut columns = Vec::new();
        for item in items {
            match item {
                SelectItem::Wildcard(_) => columns.push(ColumnRef::Star),
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    columns.push(ColumnRef::Named {
                        name: ident.value.clone(),
                        alias: None,
                    });
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(idents)) => {
                    let name = idents.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".");
                    columns.push(ColumnRef::Named { name, alias: None });
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let name = match expr {
                        Expr::Identifier(ident) => ident.value.clone(),
                        Expr::CompoundIdentifier(idents) => {
                            idents.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join(".")
                        }
                        _ => return Err(anyhow!("Complex expressions not supported")),
                    };
                    columns.push(ColumnRef::Named {
                        name,
                        alias: Some(alias.value.clone()),
                    });
                }
                _ => return Err(anyhow!("Unsupported select item: {:?}", item)),
            }
        }
        Ok(columns)
    }

    fn convert_where(&self, expr: &Expr) -> Result<WhereClause> {
        let mut conditions = Vec::new();
        self.extract_conditions(expr, &mut conditions)?;
        Ok(WhereClause { conditions })
    }

    fn extract_conditions(&self, expr: &Expr, conditions: &mut Vec<Condition>) -> Result<()> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => {
                        self.extract_conditions(left, conditions)?;
                        self.extract_conditions(right, conditions)?;
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq => {
                        let column = self.extract_column(left)?;
                        let operator = self.convert_operator(op)?;
                        let value = self.extract_value(right)?;
                        conditions.push(Condition {
                            column,
                            operator,
                            value,
                        });
                    }
                    _ => return Err(anyhow!("Unsupported operator: {:?}", op)),
                }
            }
            Expr::Like { expr, pattern, .. } => {
                let column = self.extract_column(expr)?;
                let value = self.extract_value(pattern)?;
                conditions.push(Condition {
                    column,
                    operator: Operator::Like,
                    value,
                });
            }
            Expr::InList { expr, list, .. } => {
                let column = self.extract_column(expr)?;
                let values: Result<Vec<Value>> = list.iter().map(|e| self.extract_value(e)).collect();
                conditions.push(Condition {
                    column,
                    operator: Operator::In,
                    value: Value::List(values?),
                });
            }
            _ => return Err(anyhow!("Unsupported WHERE expression: {:?}", expr)),
        }
        Ok(())
    }

    fn extract_column(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => {
                Ok(idents.iter().map(|i| i.value.as_str()).collect::<Vec<_>>().join("."))
            }
            _ => Err(anyhow!("Expected column identifier")),
        }
    }

    fn extract_value(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(v) => {
                // v is now ValueWithSpan, access .value to get the actual Value
                match &v.value {
                    SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                        Ok(Value::String(s.clone()))
                    }
                    SqlValue::Number(n, _) => {
                        let f: f64 = n.parse()?;
                        Ok(Value::Number(f))
                    }
                    SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
                    SqlValue::Null => Ok(Value::Null),
                    _ => Err(anyhow!("Unsupported value type: {:?}", v)),
                }
            }
            _ => Err(anyhow!("Expected literal value")),
        }
    }

    fn convert_operator(&self, op: &BinaryOperator) -> Result<Operator> {
        match op {
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::Ne),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::Le),
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::Ge),
            _ => Err(anyhow!("Unsupported operator: {:?}", op)),
        }
    }

    fn convert_order_by(&self, order_by: &ast::OrderBy) -> Result<Vec<OrderByExpr>> {
        let mut result = Vec::new();

        // Handle the new OrderBy structure with kind field
        match &order_by.kind {
            OrderByKind::Expressions(exprs) => {
                for expr in exprs {
                    let column = self.extract_column(&expr.expr)?;
                    let descending = expr.options.asc.map(|asc| !asc).unwrap_or(false);
                    result.push(OrderByExpr { column, descending });
                }
            }
            OrderByKind::All(_) => {
                // ORDER BY ALL - not commonly used, treat as empty for now
                return Err(anyhow!("ORDER BY ALL not supported"));
            }
        }

        Ok(result)
    }

    fn extract_limit_from_query(&self, query: &ast::Query) -> Result<Option<u64>> {
        if let Some(ref limit_clause) = query.limit_clause {
            match limit_clause {
                ast::LimitClause::LimitOffset { limit, .. } => {
                    if let Some(limit_expr) = limit {
                        return self.extract_limit_value(limit_expr).map(Some);
                    }
                }
                ast::LimitClause::OffsetCommaLimit { limit, .. } => {
                    return self.extract_limit_value(limit).map(Some);
                }
            }
        }
        Ok(None)
    }

    fn extract_limit_value(&self, expr: &Expr) -> Result<u64> {
        match expr {
            Expr::Value(v) => {
                match &v.value {
                    SqlValue::Number(n, _) => {
                        let limit: u64 = n.parse()?;
                        Ok(limit)
                    }
                    _ => Err(anyhow!("LIMIT must be a number")),
                }
            }
            _ => Err(anyhow!("LIMIT must be a number")),
        }
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let parser = SqlParser::new();
        let query = parser.parse("SELECT * FROM pods").unwrap();
        match query {
            Query::Select(select) => {
                assert_eq!(select.table_ref.table, "pods");
                assert!(select.table_ref.cluster.is_none());
                assert!(matches!(select.columns[0], ColumnRef::Star));
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parse_select_with_cluster() {
        let parser = SqlParser::new();
        let query = parser.parse("SELECT * FROM \"prod-cluster\".pods").unwrap();
        match query {
            Query::Select(select) => {
                assert_eq!(select.table_ref.table, "pods");
                assert_eq!(select.table_ref.cluster, Some("prod-cluster".to_string()));
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parse_select_with_dotted_cluster() {
        let parser = SqlParser::new();
        let query = parser.parse("SELECT * FROM \"prod.us-east-1.company.com\".pods").unwrap();
        match query {
            Query::Select(select) => {
                assert_eq!(select.table_ref.table, "pods");
                assert_eq!(select.table_ref.cluster, Some("prod.us-east-1.company.com".to_string()));
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let parser = SqlParser::new();
        let query = parser.parse("SELECT name, status FROM pods WHERE namespace = 'default'").unwrap();
        match query {
            Query::Select(select) => {
                assert_eq!(select.table_ref.table, "pods");
                assert_eq!(select.columns.len(), 2);
                assert!(select.where_clause.is_some());
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parse_use() {
        let parser = SqlParser::new();
        let query = parser.parse("USE production").unwrap();
        match query {
            Query::Use(use_q) => assert_eq!(use_q.database, "production"),
            _ => panic!("Expected USE query"),
        }
    }

    #[test]
    fn test_parse_show_tables() {
        let parser = SqlParser::new();
        let query = parser.parse("SHOW TABLES").unwrap();
        assert!(matches!(query, Query::Show(ShowQuery::Tables)));
    }

    #[test]
    fn test_parse_describe_with_cluster() {
        let parser = SqlParser::new();
        let query = parser.parse("DESCRIBE \"prod.cluster.io\".pods").unwrap();
        match query {
            Query::Describe(desc) => {
                assert_eq!(desc.table_ref.table, "pods");
                assert_eq!(desc.table_ref.cluster, Some("prod.cluster.io".to_string()));
            }
            _ => panic!("Expected DESCRIBE query"),
        }
    }
}
