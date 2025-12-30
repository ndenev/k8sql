# k8sql

Query Kubernetes clusters using SQL. Powered by [Apache DataFusion](https://datafusion.apache.org/).

## Features

- **SQL queries on Kubernetes resources** - SELECT, WHERE, ORDER BY, LIMIT, GROUP BY
- **Multi-cluster support** - Query across clusters with `_cluster` column
- **Query optimizer** - Pushes filters to K8s API (namespaces, labels)
- **Multiple interfaces** - Interactive REPL, batch mode, PostgreSQL wire protocol
- **CRD support** - Automatically discovers and caches Custom Resource Definitions

## Installation

### Quick Install (Linux/macOS)

```bash
curl -sSfL https://raw.githubusercontent.com/ndenev/k8sql/master/install.sh | sh
```

The script automatically detects your OS and architecture, downloads the latest release, and installs to `/usr/local/bin` or `~/.local/bin`.

### From Cargo

```bash
cargo install k8sql
```

### Manual Download

Download pre-built binaries for Linux, macOS, and Windows from [GitHub Releases](https://github.com/ndenev/k8sql/releases).

## Quick Start

```bash
# Interactive REPL
k8sql

# Single query
k8sql -q "SELECT name, namespace, status->>'phase' FROM pods"

# Multi-cluster query
k8sql -c "prod-*" -q "SELECT _cluster, name FROM pods WHERE status->>'phase' = 'Failed'"

# Output as JSON
k8sql -q "SELECT * FROM deployments" -o json
```

## CLI Options

```
-q, --query <SQL>       Execute a SQL query directly
-c, --context <CTX>     Kubernetes context(s): name, comma-separated, or glob pattern
-n, --namespace <NS>    Default namespace (default: "default")
-o, --output <FMT>      Output format: table, json, csv, yaml (default: table)
-f, --file <PATH>       Execute queries from a file
    --no-headers        Omit column headers in output
    --refresh-crds      Force refresh of cached CRD schemas
-v, --verbose           Enable verbose logging
```

### Subcommands

```bash
k8sql interactive       # Start REPL (default)
k8sql daemon -p 15432   # PostgreSQL wire protocol server
```

## SQL Extensions

### The `_cluster` Column

Every table includes `_cluster` as the first column, enabling cross-cluster queries:

```sql
SELECT * FROM pods WHERE _cluster = 'prod'           -- Specific cluster
SELECT * FROM pods WHERE _cluster IN ('prod', 'dev') -- Multiple clusters
SELECT * FROM pods WHERE _cluster = '*'              -- All clusters (parallel)
```

### JSON Field Access

Access nested fields in `spec`, `status`, `labels`, `annotations` using PostgreSQL-style operators:

```sql
SELECT name, status->>'phase' FROM pods
SELECT name, status->'nodeInfo'->>'kubeletVersion' FROM nodes
SELECT * FROM pods WHERE labels->>'app' = 'nginx'
```

Or function syntax for deeper nesting:

```sql
SELECT json_get_str(spec, 'containers', 0, 'image') FROM pods
SELECT json_get_int(spec, 'replicas') FROM deployments
```

Available functions: `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`, `json_get_json`, `json_get_array`, `json_length`, `json_keys`.

### Working with Arrays

Use `json_get_array()` with `UNNEST` to expand arrays:

```sql
SELECT name, json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
```

## Query Optimization

k8sql pushes predicates to the Kubernetes API when possible:

| Predicate | Optimization |
|-----------|--------------|
| `namespace = 'x'` | Namespaced API call |
| `labels->>'app' = 'nginx'` | Label selector (server-side) |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster = '*'` | Parallel queries to all clusters |

Use `-v` to see which filters are pushed down.

## Table Schema

All resources share a consistent schema:

| Column | Type | Description |
|--------|------|-------------|
| `_cluster` | text | Kubernetes context name |
| `api_version` | text | API version (v1, apps/v1, etc.) |
| `kind` | text | Resource kind |
| `name` | text | Resource name |
| `namespace` | text | Namespace (null for cluster-scoped) |
| `uid` | text | Unique identifier |
| `created` | timestamp | Creation timestamp |
| `labels` | json | Labels (access with `labels->>'key'`) |
| `annotations` | json | Annotations |
| `spec` | json | Resource specification |
| `status` | json | Resource status |

Special cases: ConfigMaps/Secrets have `data` instead of spec/status. Events have dedicated columns.

## REPL Commands

```
SHOW TABLES          List available tables
SHOW DATABASES       List kubectl contexts (* = active)
DESCRIBE <table>     Show table schema
USE <cluster>        Switch context(s) - supports globs: USE prod-*
\dt, \l, \d, \x, \q  Shortcuts
```

Context selection persists to `~/.k8sql/config.json` and restores on next startup.

## Daemon Mode

Run as a PostgreSQL-compatible server:

```bash
k8sql daemon --port 15432 --context "prod-*"
psql -h localhost -p 15432
```

## Caching

k8sql caches CRD schemas for fast startup:

- **CRD schemas**: Cached indefinitely (use `--refresh-crds` to force refresh)
- **Cluster CRD list**: Checked hourly for new/removed CRDs
- **Cache location**: `~/.k8sql/cache/`

## Example Queries

```sql
-- Pods with status
SELECT name, namespace, status->>'phase' as phase FROM pods

-- Unhealthy deployments
SELECT name, json_get_int(spec, 'replicas') as desired,
       json_get_int(status, 'readyReplicas') as ready
FROM deployments
WHERE json_get_int(status, 'readyReplicas') < json_get_int(spec, 'replicas')

-- Cross-cluster pod count
SELECT _cluster, COUNT(*) FROM pods WHERE _cluster = '*' GROUP BY _cluster

-- Find pods by label (server-side filtering)
SELECT name FROM pods WHERE labels->>'app' = 'nginx' AND namespace = 'default'
```

## Built With

- [Apache DataFusion](https://datafusion.apache.org/) - SQL query engine
- [kube-rs](https://kube.rs/) - Kubernetes client for Rust

## License

BSD-3-Clause
