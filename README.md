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

### JSON Path Access

Access nested fields using dot notation:

```sql
-- Access nested status fields
SELECT name, status.phase, status.podIP FROM pods

-- Access spec fields
SELECT name, spec.replicas, status.readyReplicas FROM deployments

-- Access container information
SELECT name, spec.containers FROM pods

-- Access node information
SELECT name, status.nodeInfo.kubeletVersion FROM nodes
```

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

-- Specific columns
SELECT name, namespace, phase, node FROM pods

-- With ordering and limit
SELECT name, namespace, phase FROM pods ORDER BY name LIMIT 10

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

-- Status filtering
SELECT name, namespace FROM pods WHERE phase = 'Running'
SELECT name, namespace FROM pods WHERE phase != 'Running'

-- Numeric comparisons
SELECT name, replicas, ready FROM deployments WHERE replicas > 1
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
-- Pods with restart counts
SELECT name, namespace, phase, restarts, node FROM pods

-- Deployments with replica status
SELECT name, namespace, replicas, ready, available FROM deployments

-- Services with their types
SELECT name, namespace, type, cluster_ip FROM services

-- PVCs with storage info
SELECT name, namespace, phase, storage_class, capacity FROM persistentvolumeclaims

-- Nodes with version info
SELECT name, version, os, arch FROM nodes

-- Recent events
SELECT name, namespace, type, reason, message, count FROM events ORDER BY created DESC LIMIT 20
```

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

CRDs are automatically discovered and available using their plural name (e.g., `certificates`, `issuers` for cert-manager).

## Query Planner

k8sql optimizes queries by pushing predicates to the Kubernetes API when possible:

| Predicate | Optimization |
|-----------|--------------|
| `namespace = 'x'` | Uses `Api::namespaced()` |
| `labels.app = 'nginx'` | K8s label selector |
| `status.phase = 'Running'` | K8s field selector |
| `namespace LIKE '%'` | Queries all, filters client-side |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster = '*'` | Queries all clusters |

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
