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
k8sql -q "SELECT name, namespace, status.phase FROM pods"

# Query specific namespace (optimized - pushed to API)
k8sql -q "SELECT * FROM pods WHERE namespace = 'kube-system'"

# Query with pattern matching (client-side filter)
k8sql -q "SELECT * FROM pods WHERE namespace LIKE 'kube%'"

# Query specific cluster
k8sql -q "SELECT * FROM pods WHERE _cluster = 'prod'"

# Query all clusters
k8sql -q "SELECT * FROM pods WHERE _cluster = '*'"

# Output as JSON
k8sql -q "SELECT * FROM deployments" -o json
```

## Supported Resources

| Table | Aliases | Scope |
|-------|---------|-------|
| pods | pod | Namespace |
| services | service, svc | Namespace |
| deployments | deployment, deploy | Namespace |
| configmaps | configmap, cm | Namespace |
| secrets | secret | Namespace |
| ingresses | ingress, ing | Namespace |
| jobs | job | Namespace |
| cronjobs | cronjob, cj | Namespace |
| statefulsets | statefulset, sts | Namespace |
| daemonsets | daemonset, ds | Namespace |
| pvcs | pvc, persistentvolumeclaim | Namespace |
| nodes | node | Cluster |
| namespaces | namespace, ns | Cluster |
| pvs | pv, persistentvolume | Cluster |

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
