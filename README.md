# k8sql

Query Kubernetes clusters using SQL or [PRQL](https://prql-lang.org). Powered by [Apache DataFusion](https://datafusion.apache.org/).

## Features

- **Dual query languages** — Write SQL or [PRQL](https://prql-lang.org), auto-detected
- **Intuitive JSON paths** — Use `status.phase` instead of `status->>'phase'`
- **Multi-cluster queries** — Query across clusters with `_cluster = '*'`
- **Smart optimization** — Filters pushed to K8s API (namespaces, labels, field selectors)
- **Multiple interfaces** — Interactive REPL, batch mode, PostgreSQL wire protocol
- **Full CRD support** — Automatically discovers Custom Resource Definitions

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

# SQL query
k8sql -q "SELECT name, namespace, status.phase FROM pods"

# PRQL query (auto-detected)
k8sql -q "from pods | filter status.phase == 'Running' | select {name, namespace}"

# Multi-cluster query
k8sql -c "prod-*" -q "SELECT _cluster, name FROM pods WHERE status.phase = 'Failed'"

# Output as JSON (or YAML or CSV)
k8sql -q "SELECT * FROM deployments" -o json
```

## SQL vs PRQL

k8sql supports both SQL and [PRQL](https://prql-lang.org) — just write your query and it's auto-detected:

| Task | SQL | PRQL |
|------|-----|------|
| Basic query | `SELECT name FROM pods LIMIT 5` | `from pods \| select {name} \| take 5` |
| Filter | `WHERE namespace = 'default'` | `filter namespace == "default"` |
| Sort descending | `ORDER BY created DESC` | `sort {-created}` |
| Aggregate | `COUNT(*) ... GROUP BY namespace` | `group namespace (aggregate {count this})` |
| JSON field | `status.phase` or `status->>'phase'` | `status.phase` |
| Regex match | `WHERE name ~ 'nginx.*'` | `filter name ~= "nginx.*"` |
| Null coalesce | `COALESCE(namespace, 'default')` | `namespace ?? "default"` |

## CLI Options

```
-q, --query <SQL>       Execute a SQL query directly
-c, --context <CTX>     Kubernetes context(s): name, comma-separated, or glob pattern
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

### Intuitive JSON Access

Intuitive JSON path access vs the more verbose `->` operators.


```sql
-- Intuitive syntax (auto-converted)
SELECT name, status.phase FROM pods
SELECT name, spec.containers[0].image FROM pods
SELECT * FROM pods WHERE labels.app = 'nginx'

-- Array expansion (one row per container)
SELECT name, spec.containers[].image FROM pods
```

This works in both SQL and PRQL - no special syntax needed.

**Supported patterns:**

| Syntax | Meaning | Converted To |
|--------|---------|--------------|
| `status.phase` | Field access | `status->>'phase'` |
| `spec.selector.app` | Nested fields | `spec->'selector'->>'app'` |
| `spec.containers[0]` | Array index | `spec->'containers'->0` |
| `spec.containers[]` | Array expansion | `UNNEST(json_get_array(...))` |

**Advanced: PostgreSQL arrow operators** (also supported):

```sql
SELECT name, status->>'phase' FROM pods
SELECT name, status->'nodeInfo'->>'kubeletVersion' FROM nodes
```

**JSON functions** for complex access:

```sql
SELECT json_get_str(spec, 'containers', 0, 'image') FROM pods
SELECT json_get_int(spec, 'replicas') FROM deployments
```

Available JSON functions: `json_get_str`, `json_get_int`, `json_get_float`, `json_get_bool`, `json_get_json`, `json_get_array`, `json_length`, `json_keys`.

## Why PRQL?

[PRQL](https://prql-lang.org) (Pipelined Relational Query Language) offers a more readable alternative to SQL:

- **Readable pipelines** — Data flows left-to-right: `from pods | filter ... | select ...`
- **Less repetition** — No need to repeat columns in GROUP BY
- **Composable** — Build complex queries incrementally
- **Same JSON paths** — Use `status.phase` just like in SQL

**PRQL-specific operators:**

| Operator | Meaning | Example |
|----------|---------|---------|
| `~=` | Regex match | `filter name ~= "nginx.*"` |
| `??` | Null coalesce | `derive ns = namespace ?? "default"` |
| `f"..."` | String interpolation | `derive full = f"{namespace}/{name}"` |
| `-col` | Sort descending | `sort {-created}` |

PRQL is auto-detected when queries start with `from`, `let`, or `prql`.

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

Special cases: ConfigMaps/Secrets have `data` instead of spec/status. Events have dedicated columns, etc.
CRDs have their top level fields discovered.

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

### SQL

```sql
-- Pods with status (using intuitive dot notation)
SELECT name, namespace, status.phase FROM pods

-- Container images from pods
SELECT name, spec.containers[0].image FROM pods WHERE namespace = 'default'

-- Unhealthy deployments
SELECT name, spec.replicas as desired, status.readyReplicas as ready
FROM deployments
WHERE status.readyReplicas < spec.replicas

-- Cross-cluster pod count
SELECT _cluster, COUNT(*) FROM pods WHERE _cluster = '*' GROUP BY _cluster

-- Find pods by label (server-side filtering)
SELECT name FROM pods WHERE labels.app = 'nginx' AND namespace = 'default'

-- All container images (array expansion)
SELECT name, spec.containers[].image as container_image FROM pods
```

### PRQL

```prql
# Pods with status
from pods | select {name, namespace, phase = status.phase}

# Filter by phase
from pods
filter status.phase == "Running"
select {name, namespace}
take 10

# Unhealthy deployments
from deployments
filter status.readyReplicas < spec.replicas
select {name, desired = spec.replicas, ready = status.readyReplicas}

# Pods by label
from pods
filter labels.app == "nginx" && namespace == "default"
select {name, namespace}

# Aggregation - pod count by namespace
from pods
group namespace (aggregate {count = count this})
sort {-count}
```

## Built With

- [Apache DataFusion](https://datafusion.apache.org/) - SQL query engine
- [kube-rs](https://kube.rs/) - Kubernetes client for Rust

## License

BSD-3-Clause
