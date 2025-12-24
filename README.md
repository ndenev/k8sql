# k8sql

Query Kubernetes clusters using SQL. k8sql exposes Kubernetes resources as database tables, kubectl contexts as databases, and includes a query planner that optimizes API calls.

## Features

- **SQL queries on Kubernetes resources** - SELECT, WHERE, ORDER BY, LIMIT
- **Multi-cluster support** - Query across clusters using `_cluster` column
- **Query optimizer** - Pushes filters to K8s API (namespaces, labels, field selectors)
- **Multiple interfaces** - Interactive REPL, batch mode, PostgreSQL wire protocol
- **Rich output formats** - Table, JSON, CSV, YAML

## Quick Start

```bash
# Interactive REPL
k8sql

# Single query
k8sql -q "SELECT name, namespace, phase FROM pods"

# Output as JSON
k8sql -q "SELECT * FROM deployments" -o json

# Use specific context
k8sql -c prod-cluster -q "SELECT * FROM pods"
```

## SQL Extensions

### The `_cluster` Column

Every table includes a virtual `_cluster` column as the first column, representing the kubectl context (cluster) each resource belongs to. This enables cross-cluster queries.

```sql
-- Query only the current context (default behavior)
SELECT name, namespace FROM pods

-- Query a specific cluster
SELECT * FROM pods WHERE _cluster = 'prod-cluster'

-- Query multiple specific clusters
SELECT * FROM pods WHERE _cluster IN ('prod', 'staging')

-- Query ALL clusters in your kubeconfig
SELECT * FROM pods WHERE _cluster = '*'
```

The `'*'` wildcard is a special value that expands to all available kubectl contexts, executing parallel queries across all clusters.

### Querying Nested JSON Fields

Kubernetes resources are exposed with `spec` and `status` as JSON columns. Query nested fields using either **PostgreSQL-style operators** or **function syntax**:

#### PostgreSQL-Style Operators

```sql
-- Extract as text with ->>
SELECT name, status->>'phase' as phase FROM pods

-- Chain operators for nested access
SELECT name, status->'nodeInfo'->>'kubeletVersion' as version FROM nodes

-- Filter on JSON fields
SELECT name FROM pods WHERE status->>'phase' = 'Running'
```

#### Function Syntax

```sql
-- Extract string values with json_get_str(column, key1, key2, ...)
SELECT name, json_get_str(status, 'phase') as phase FROM pods

-- Access nested fields by chaining keys
SELECT name, json_get_str(spec, 'nodeName') as node FROM pods

-- Access array elements by index (0-based)
SELECT name, json_get_str(spec, 'containers', 0, 'image') as image FROM pods

-- Access deeply nested fields
SELECT name, json_get_str(status, 'nodeInfo', 'kubeletVersion') as version FROM nodes

-- Get integer values
SELECT name, json_get_int(spec, 'replicas') as replicas FROM deployments

-- Get the full JSON object as string
SELECT name, json_get_json(spec, 'containers') as containers FROM pods
```

#### Available JSON Functions and Operators

| Operator/Function | Description |
|-------------------|-------------|
| `->` | Get JSON value (returns union type) |
| `->>` | Get value as text string |
| `json_get_str(json, key, ...)` | Extract as string |
| `json_get_int(json, key, ...)` | Extract as integer |
| `json_get_float(json, key, ...)` | Extract as float |
| `json_get_bool(json, key, ...)` | Extract as boolean |
| `json_get_json(json, key, ...)` | Extract nested JSON as string |
| `json_contains(json, key, ...)` | Check if key exists |

The function syntax supports chaining multiple keys and array indices in a single call, making it convenient for deeply nested access.

### Label Queries

Query resources by labels using the `labels.` prefix:

```sql
-- Filter by label (pushed to K8s API as label selector)
SELECT * FROM pods WHERE labels.app = 'nginx'

-- Multiple label conditions
SELECT * FROM pods WHERE labels.app = 'nginx' AND labels.env = 'prod'
```

## Example Queries

### Basic Queries

```sql
-- All pods in current context
SELECT * FROM pods

-- Specific columns including API version and kind
SELECT api_version, kind, name, namespace FROM pods LIMIT 10

-- With ordering and limit
SELECT name, namespace, json_get_str(status, 'phase') as phase FROM pods ORDER BY name LIMIT 10

-- Descending order
SELECT name, created FROM pods ORDER BY created DESC
```

### Filtering

```sql
-- Exact namespace match (optimized - uses namespaced API)
SELECT * FROM pods WHERE namespace = 'kube-system'

-- Pattern matching (client-side filter)
SELECT * FROM pods WHERE namespace LIKE 'kube%'
SELECT * FROM pods WHERE name LIKE '%nginx%'

-- Filter on JSON fields
SELECT name, namespace FROM pods
WHERE json_get_str(status, 'phase') = 'Running'

-- Numeric comparisons on JSON
SELECT name, json_get_int(spec, 'replicas') as replicas
FROM deployments
WHERE json_get_int(spec, 'replicas') > 1
```

### Cross-Cluster Queries

```sql
-- Compare pods across prod and staging
SELECT _cluster, name, namespace, phase
FROM pods
WHERE _cluster IN ('prod', 'staging') AND namespace = 'default'

-- Find all failing pods across all clusters
SELECT _cluster, name, namespace, phase
FROM pods
WHERE _cluster = '*' AND phase = 'Failed'

-- Count deployments per cluster (future feature)
SELECT _cluster, COUNT(*) FROM deployments WHERE _cluster = '*' GROUP BY _cluster
```

### Resource-Specific Examples

```sql
-- Pods with status info
SELECT name, namespace,
       json_get_str(status, 'phase') as phase,
       json_get_str(spec, 'nodeName') as node
FROM pods

-- Deployments with replica status
SELECT name, namespace,
       json_get_int(spec, 'replicas') as desired,
       json_get_int(status, 'readyReplicas') as ready,
       json_get_int(status, 'availableReplicas') as available
FROM deployments

-- Find pods running a specific image
SELECT name, namespace, json_get_str(spec, 'containers', 0, 'image') as image
FROM pods
WHERE json_get_str(spec, 'containers', 0, 'image') LIKE '%nginx%'

-- Services with their types
SELECT name, namespace,
       json_get_str(spec, 'type') as type,
       json_get_str(spec, 'clusterIP') as cluster_ip
FROM services

-- Nodes with version info
SELECT name,
       json_get_str(status, 'nodeInfo', 'kubeletVersion') as version,
       json_get_str(status, 'nodeInfo', 'osImage') as os
FROM nodes

-- Recent events (events have dedicated columns)
SELECT name, namespace, type, reason, message, count
FROM events
ORDER BY created DESC LIMIT 20

-- CRDs work the same way
SELECT name, json_get_str(spec, 'secretName') as secret
FROM certificates
```

## Table Schema

All Kubernetes resources are exposed with a consistent schema:

| Column | Type | Description |
|--------|------|-------------|
| `_cluster` | text | Kubernetes context/cluster name |
| `api_version` | text | API version (e.g., `v1`, `apps/v1`, `cert-manager.io/v1`) |
| `kind` | text | Resource kind (e.g., `Pod`, `Deployment`, `Certificate`) |
| `name` | text | Resource name |
| `namespace` | text | Namespace (null for cluster-scoped resources) |
| `uid` | text | Unique identifier |
| `created` | timestamp | Creation timestamp |
| `labels` | json | Resource labels as JSON object |
| `annotations` | json | Resource annotations as JSON object |
| `spec` | json | Resource specification (desired state) |
| `status` | json | Resource status (current state) |

The `api_version` and `kind` columns are self-describing - they identify exactly what type of resource each row represents. This is especially useful when querying CRDs across multiple clusters that might have different versions installed.

Special cases:
- **ConfigMaps/Secrets**: Have `data` column instead of spec/status
- **Events**: Have dedicated columns for `type`, `reason`, `message`, `count`, etc.
- **PodMetrics/NodeMetrics**: Have `timestamp`, `window`, and `containers`/`usage` columns (see below)

Use `DESCRIBE <table>` to see the exact schema for any resource.

## Metrics Tables

If the Kubernetes metrics-server is installed, k8sql exposes pod and node metrics:

| Table | Description |
|-------|-------------|
| `podmetrics` | CPU/memory usage per pod (from `metrics.k8s.io`) |
| `nodemetrics` | CPU/memory usage per node (from `metrics.k8s.io`) |

### PodMetrics Schema

| Column | Type | Description |
|--------|------|-------------|
| `name` | text | Pod name |
| `namespace` | text | Namespace |
| `timestamp` | timestamp | When metrics were collected |
| `window` | text | Measurement time window |
| `containers` | json | Array of container metrics |

### NodeMetrics Schema

| Column | Type | Description |
|--------|------|-------------|
| `name` | text | Node name |
| `timestamp` | timestamp | When metrics were collected |
| `window` | text | Measurement time window |
| `usage` | json | Node resource usage (cpu, memory) |

### Metrics Query Examples

```sql
-- Pod CPU and memory usage
SELECT name, namespace,
       json_get_str(containers, 0, 'usage', 'cpu') as cpu,
       json_get_str(containers, 0, 'usage', 'memory') as memory
FROM podmetrics

-- Node resource usage
SELECT name,
       json_get_str(usage, 'cpu') as cpu,
       json_get_str(usage, 'memory') as memory
FROM nodemetrics

-- Find pods using most memory (note: values are strings like "123456Ki")
SELECT name, namespace,
       json_get_str(containers, 0, 'usage', 'memory') as memory
FROM podmetrics
ORDER BY memory DESC
LIMIT 10
```

Note: CPU values are in nanocores (e.g., `"2083086n"` = ~2 millicores), memory in kibibytes (e.g., `"44344Ki"`).

## Supported Resources

k8sql discovers all available resources at runtime, including CRDs. Use `SHOW TABLES` to see what's available in your cluster.

Common resources with aliases:

| Table | Aliases | Scope |
|-------|---------|-------|
| pods | pod | Namespaced |
| services | service, svc | Namespaced |
| deployments | deployment, deploy | Namespaced |
| configmaps | configmap, cm | Namespaced |
| secrets | secret | Namespaced |
| ingresses | ingress, ing | Namespaced |
| jobs | job | Namespaced |
| cronjobs | cronjob, cj | Namespaced |
| statefulsets | statefulset, sts | Namespaced |
| daemonsets | daemonset, ds | Namespaced |
| persistentvolumeclaims | pvc | Namespaced |
| nodes | node | Cluster |
| namespaces | namespace, ns | Cluster |
| persistentvolumes | pv | Cluster |
| podmetrics | - | Namespaced |
| nodemetrics | - | Cluster |

CRDs are automatically discovered and available using their plural name (e.g., `certificates`, `issuers` for cert-manager).

## Query Planner

k8sql optimizes queries by pushing predicates to the Kubernetes API when possible:

| Predicate | Optimization |
|-----------|--------------|
| `namespace = 'x'` | Uses `Api::namespaced()` |
| `labels.app = 'nginx'` | K8s label selector |
| `namespace LIKE '%'` | Queries all, filters client-side |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster = '*'` | Queries all clusters |
| `json_get_str(...)` | Client-side JSON parsing |

Note: JSON field filtering (e.g., `WHERE json_get_str(status, 'phase') = 'Running'`) is performed client-side after fetching resources.

## REPL Commands

```
SHOW TABLES          - List available tables
SHOW DATABASES       - List kubectl contexts
DESCRIBE <table>     - Show table schema
USE <cluster>        - Switch to cluster
\dt                  - Shortcut for SHOW TABLES
\l                   - Shortcut for SHOW DATABASES
\d <table>           - Shortcut for DESCRIBE
\q                   - Quit
```

## Daemon Mode

Run as a PostgreSQL-compatible server:

```bash
k8sql daemon --port 15432

# Connect with psql
psql -h localhost -p 15432
```

## Building

```bash
cargo build --release
```

## License

BSD-3-Clause - Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
