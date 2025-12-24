use anyhow::Result;
use futures::future::join_all;

use crate::kubernetes::K8sClientPool;
use crate::output::QueryResult;
use super::ast::*;
use super::planner::{ClusterScope, NamespaceScope, QueryPlan, QueryPlanner};

pub struct QueryExecutor {
    pool: K8sClientPool,
    planner: QueryPlanner,
}

impl QueryExecutor {
    pub fn new(pool: K8sClientPool) -> Self {
        Self {
            pool,
            planner: QueryPlanner::new(),
        }
    }

    pub async fn execute(&self, query: Query) -> Result<QueryResult> {
        match query {
            Query::Select(select) => self.execute_select(select).await,
            Query::Show(show) => self.execute_show(show).await,
            Query::Use(use_q) => self.execute_use(use_q).await,
            Query::Describe(desc) => self.execute_describe(desc).await,
        }
    }

    /// Get the list of cluster names to query based on cluster scope
    async fn resolve_clusters(&self, scope: &ClusterScope) -> Result<Vec<String>> {
        match scope {
            ClusterScope::Current => {
                let current = self.pool.current_context().await?;
                Ok(vec![current])
            }
            ClusterScope::Specific(clusters) => Ok(clusters.clone()),
            ClusterScope::All => self.pool.list_contexts(),
        }
    }

    async fn execute_select(&self, query: SelectQuery) -> Result<QueryResult> {
        // Use the query planner to optimize the query
        let plan = self.planner.plan(&query);

        // Resolve clusters to query
        let clusters = self.resolve_clusters(&plan.cluster_scope).await?;

        // Determine namespace for API calls
        let namespace = match &plan.namespace_scope {
            NamespaceScope::All => None,
            NamespaceScope::Specific(ns) => Some(ns.clone()),
        };

        // Query all clusters in parallel with optimized API filters
        let table = plan.table.clone();
        let api_filters = plan.api_filters.clone();

        let futures: Vec<_> = clusters
            .iter()
            .map(|cluster| {
                let table = table.clone();
                let ns = namespace.clone();
                let cluster = cluster.clone();
                let filters = api_filters.clone();
                async move {
                    let resources = self
                        .pool
                        .fetch_resources(&table, ns.as_deref(), Some(&cluster), &filters)
                        .await?;
                    Ok::<_, anyhow::Error>((cluster, resources))
                }
            })
            .collect();

        let results = join_all(futures).await;

        // Collect all resources with their cluster names
        let mut all_resources: Vec<(String, serde_json::Value)> = Vec::new();
        for result in results {
            let (cluster, resources) = result?;
            for resource in resources {
                all_resources.push((cluster.clone(), resource));
            }
        }

        // Apply client-side filters (only those not pushed to API)
        let filtered = self.apply_client_filters(all_resources, &plan)?;

        // Apply ORDER BY
        let ordered = self.apply_order_by_with_cluster(filtered, &query.order_by)?;

        // Apply LIMIT
        let limited = if let Some(limit) = query.limit {
            ordered.into_iter().take(limit as usize).collect()
        } else {
            ordered
        };

        // Project columns (including _cluster)
        let (columns, rows) = self.project_columns_with_cluster(&query.columns, &plan.table, limited)?;

        Ok(QueryResult { columns, rows })
    }

    fn get_field_value(&self, resource: &serde_json::Value, column: &str) -> Option<serde_json::Value> {
        // Handle dotted paths like "metadata.name"
        let parts: Vec<&str> = column.split('.').collect();
        let mut current = resource;

        for part in parts {
            match current.get(part) {
                Some(v) => current = v,
                None => return None,
            }
        }
        Some(current.clone())
    }

    fn matches_condition(
        &self,
        field_value: &Option<serde_json::Value>,
        operator: &Operator,
        condition_value: &Value,
    ) -> Result<bool> {
        let Some(fv) = field_value else {
            return Ok(matches!(operator, Operator::Eq) && matches!(condition_value, Value::Null));
        };

        match operator {
            Operator::Eq => Ok(self.values_equal(fv, condition_value)),
            Operator::Ne => Ok(!self.values_equal(fv, condition_value)),
            Operator::Like => {
                if let (serde_json::Value::String(s), Value::String(pattern)) = (fv, condition_value) {
                    let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
                    let re = regex::Regex::new(&format!("^{}$", regex_pattern))?;
                    Ok(re.is_match(s))
                } else {
                    Ok(false)
                }
            }
            Operator::In => {
                if let Value::List(values) = condition_value {
                    Ok(values.iter().any(|v| self.values_equal(fv, v)))
                } else {
                    Ok(false)
                }
            }
            Operator::Lt | Operator::Le | Operator::Gt | Operator::Ge => {
                self.compare_values(fv, condition_value, operator)
            }
        }
    }

    fn values_equal(&self, json_val: &serde_json::Value, cond_val: &Value) -> bool {
        match (json_val, cond_val) {
            (serde_json::Value::String(s), Value::String(v)) => s == v,
            (serde_json::Value::Number(n), Value::Number(v)) => {
                n.as_f64().map(|f| (f - v).abs() < f64::EPSILON).unwrap_or(false)
            }
            (serde_json::Value::Bool(b), Value::Bool(v)) => b == v,
            (serde_json::Value::Null, Value::Null) => true,
            _ => false,
        }
    }

    fn compare_values(
        &self,
        json_val: &serde_json::Value,
        cond_val: &Value,
        op: &Operator,
    ) -> Result<bool> {
        match (json_val, cond_val) {
            (serde_json::Value::Number(n), Value::Number(v)) => {
                let f = n.as_f64().unwrap_or(0.0);
                Ok(match op {
                    Operator::Lt => f < *v,
                    Operator::Le => f <= *v,
                    Operator::Gt => f > *v,
                    Operator::Ge => f >= *v,
                    _ => false,
                })
            }
            (serde_json::Value::String(s), Value::String(v)) => {
                Ok(match op {
                    Operator::Lt => s < v,
                    Operator::Le => s <= v,
                    Operator::Gt => s > v,
                    Operator::Ge => s >= v,
                    _ => false,
                })
            }
            _ => Ok(false),
        }
    }

    // === Client-side filtering using QueryPlan ===

    fn apply_client_filters(
        &self,
        resources: Vec<(String, serde_json::Value)>,
        plan: &QueryPlan,
    ) -> Result<Vec<(String, serde_json::Value)>> {
        if plan.client_filters.conditions.is_empty() {
            return Ok(resources);
        }

        let mut filtered = Vec::new();
        for (cluster, resource) in resources {
            if self.matches_client_conditions(&resource, &plan.client_filters.conditions)? {
                filtered.push((cluster, resource));
            }
        }
        Ok(filtered)
    }

    fn matches_client_conditions(
        &self,
        resource: &serde_json::Value,
        conditions: &[Condition],
    ) -> Result<bool> {
        for cond in conditions {
            // Map column names to actual JSON paths
            let json_path = self.column_to_json_path(&cond.column);
            let field_value = self.get_field_value(resource, &json_path);

            if !self.matches_condition(&field_value, &cond.operator, &cond.value)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Map user-friendly column names to actual JSON paths
    fn column_to_json_path(&self, column: &str) -> String {
        match column {
            "name" => "metadata.name".to_string(),
            "namespace" => "metadata.namespace".to_string(),
            "labels" => "metadata.labels".to_string(),
            "annotations" => "metadata.annotations".to_string(),
            "age" | "created" => "metadata.creationTimestamp".to_string(),
            // For labels.xxx, convert to metadata.labels.xxx
            col if col.starts_with("labels.") => {
                format!("metadata.{}", col)
            }
            // Everything else passes through as-is
            other => other.to_string(),
        }
    }

    fn apply_order_by_with_cluster(
        &self,
        mut resources: Vec<(String, serde_json::Value)>,
        order_by: &[OrderByExpr],
    ) -> Result<Vec<(String, serde_json::Value)>> {
        if order_by.is_empty() {
            return Ok(resources);
        }

        resources.sort_by(|(cluster_a, a), (cluster_b, b)| {
            for expr in order_by {
                // Special handling for _cluster column
                if expr.column == "_cluster" {
                    let cmp = cluster_a.cmp(cluster_b);
                    if cmp != std::cmp::Ordering::Equal {
                        return if expr.descending { cmp.reverse() } else { cmp };
                    }
                    continue;
                }

                let va = self.get_field_value(a, &expr.column);
                let vb = self.get_field_value(b, &expr.column);

                let cmp = self.compare_json_values(&va, &vb);
                if cmp != std::cmp::Ordering::Equal {
                    return if expr.descending { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(resources)
    }

    fn project_columns_with_cluster(
        &self,
        columns: &[ColumnRef],
        table: &str,
        resources: Vec<(String, serde_json::Value)>,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let schema = crate::kubernetes::resources::get_table_schema(table);

        // Determine which columns to show
        let display_columns: Vec<(String, String)> = if columns.iter().any(|c| matches!(c, ColumnRef::Star)) {
            // SELECT * includes all columns from schema (which now includes _cluster)
            schema.columns.into_iter().map(|c| (c.name.clone(), c.name)).collect()
        } else {
            columns
                .iter()
                .filter_map(|c| match c {
                    ColumnRef::Named { name, alias } => {
                        Some((alias.clone().unwrap_or_else(|| name.clone()), name.clone()))
                    }
                    ColumnRef::Star => None,
                })
                .collect()
        };

        let column_names: Vec<String> = display_columns.iter().map(|(alias, _)| alias.clone()).collect();

        let rows: Vec<Vec<String>> = resources
            .iter()
            .map(|(cluster, r)| {
                display_columns
                    .iter()
                    .map(|(_, path)| {
                        // Special handling for _cluster column
                        if path == "_cluster" {
                            cluster.clone()
                        } else {
                            self.extract_display_value(r, path)
                        }
                    })
                    .collect()
            })
            .collect();

        Ok((column_names, rows))
    }

    fn compare_json_values(
        &self,
        a: &Option<serde_json::Value>,
        b: &Option<serde_json::Value>,
    ) -> std::cmp::Ordering {
        match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(va), Some(vb)) => {
                match (va, vb) {
                    (serde_json::Value::String(sa), serde_json::Value::String(sb)) => sa.cmp(sb),
                    (serde_json::Value::Number(na), serde_json::Value::Number(nb)) => {
                        let fa = na.as_f64().unwrap_or(0.0);
                        let fb = nb.as_f64().unwrap_or(0.0);
                        fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    _ => std::cmp::Ordering::Equal,
                }
            }
        }
    }

    fn extract_display_value(&self, resource: &serde_json::Value, path: &str) -> String {
        // Handle common field mappings
        let actual_path = match path {
            "name" => "metadata.name",
            "namespace" => "metadata.namespace",
            "age" | "created" => "metadata.creationTimestamp",
            "labels" => "metadata.labels",
            "annotations" => "metadata.annotations",
            _ => path,
        };

        let value = self.get_field_value(resource, actual_path);

        match value {
            Some(v) => self.format_value(&v, path),
            None => "<none>".to_string(),
        }
    }

    fn format_value(&self, value: &serde_json::Value, field: &str) -> String {
        match value {
            serde_json::Value::String(s) => {
                if field == "age" || field == "created" {
                    self.format_age(s)
                } else {
                    s.clone()
                }
            }
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => "<none>".to_string(),
            serde_json::Value::Array(arr) => {
                format!("[{}]", arr.len())
            }
            serde_json::Value::Object(obj) => {
                if obj.is_empty() {
                    "<none>".to_string()
                } else {
                    let pairs: Vec<String> = obj
                        .iter()
                        .take(3)
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect();
                    if obj.len() > 3 {
                        format!("{}...", pairs.join(","))
                    } else {
                        pairs.join(",")
                    }
                }
            }
        }
    }

    fn format_age(&self, timestamp: &str) -> String {
        use chrono::{DateTime, Utc};

        let Ok(created) = DateTime::parse_from_rfc3339(timestamp) else {
            return timestamp.to_string();
        };

        let now = Utc::now();
        let duration = now.signed_duration_since(created);

        let secs = duration.num_seconds();
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m", secs / 60)
        } else if secs < 86400 {
            format!("{}h", secs / 3600)
        } else {
            format!("{}d", secs / 86400)
        }
    }

    async fn execute_show(&self, show: ShowQuery) -> Result<QueryResult> {
        match show {
            ShowQuery::Tables => {
                let tables = crate::kubernetes::resources::list_tables();
                Ok(QueryResult {
                    columns: vec!["table_name".to_string(), "resource".to_string()],
                    rows: tables.into_iter().map(|(name, resource)| vec![name, resource]).collect(),
                })
            }
            ShowQuery::Databases => {
                let contexts = self.pool.list_contexts()?;
                let current = self.pool.current_context().await?;
                Ok(QueryResult {
                    columns: vec!["context".to_string(), "current".to_string()],
                    rows: contexts
                        .into_iter()
                        .map(|c| {
                            let is_current = if c == current { "*" } else { "" };
                            vec![c, is_current.to_string()]
                        })
                        .collect(),
                })
            }
        }
    }

    async fn execute_use(&self, use_q: UseQuery) -> Result<QueryResult> {
        self.pool.switch_context(&use_q.database).await?;
        Ok(QueryResult {
            columns: vec!["message".to_string()],
            rows: vec![vec![format!("Switched to context: {}", use_q.database)]],
        })
    }

    async fn execute_describe(&self, desc: DescribeQuery) -> Result<QueryResult> {
        // Note: DESCRIBE currently ignores cluster context since schemas are the same
        // across clusters. In the future, we could verify the cluster is reachable.
        let schema = crate::kubernetes::resources::get_table_schema(&desc.table_ref.table);
        Ok(QueryResult {
            columns: vec!["column".to_string(), "type".to_string(), "description".to_string()],
            rows: schema
                .columns
                .into_iter()
                .map(|c| vec![c.name, c.data_type, c.description])
                .collect(),
        })
    }
}
