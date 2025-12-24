/// Represents a table reference with optional cluster context
#[derive(Debug, Clone)]
pub struct TableRef {
    /// Cluster context (from "cluster".table syntax), None means current context
    pub cluster: Option<String>,
    /// Table name (Kubernetes resource type)
    pub table: String,
}

/// Represents a parsed SQL query in our internal representation
#[derive(Debug, Clone)]
pub enum Query {
    Select(SelectQuery),
    Show(ShowQuery),
    Use(UseQuery),
    Describe(DescribeQuery),
}

#[derive(Debug, Clone)]
pub struct SelectQuery {
    /// Columns to select (* means all)
    pub columns: Vec<ColumnRef>,
    /// Table reference with optional cluster context
    pub table_ref: TableRef,
    /// WHERE conditions
    pub where_clause: Option<WhereClause>,
    /// ORDER BY
    pub order_by: Vec<OrderByExpr>,
    /// LIMIT
    pub limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum ColumnRef {
    /// Select all columns
    Star,
    /// Named column, possibly with alias
    Named { name: String, alias: Option<String> },
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum Operator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    In,
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    List(Vec<Value>),
}

#[derive(Debug, Clone)]
pub struct OrderByExpr {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone)]
pub enum ShowQuery {
    Tables,
    Databases,
}

#[derive(Debug, Clone)]
pub struct UseQuery {
    pub database: String,
}

#[derive(Debug, Clone)]
pub struct DescribeQuery {
    pub table_ref: TableRef,
}
