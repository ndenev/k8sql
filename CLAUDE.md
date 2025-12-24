# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Priority #1: CORRECTNESS

**Correctness is our number one priority.** No hacks, no workarounds, no shortcuts.

- Code must be maximally correct - if something feels wrong, it probably is
- Match established semantics (Kubernetes, SQL/PostgreSQL, Rust idioms)
- Special cases are acceptable ONLY when they reflect actual underlying semantics (e.g., ConfigMaps have `data` not `spec` because that's how K8s works)
- NEVER assume or decide to take shortcuts without consulting me first
- Any deviations from standards, best practices, or established patterns must be discussed before implementation
- Anything that would trigger the "spidey senses" of a very senior engineer/architect should make you pause and ask
- When in doubt, ask - do not proceed with assumptions

## Priority #2: Performance

Performance is our second priority - still very important.

- Minimize API calls to Kubernetes (push filters to API when possible)
- Parallel queries across clusters
- Efficient data structures and algorithms
- But never sacrifice correctness for performance

## Project Overview

k8sql exposes the Kubernetes API as a SQL-compatible database using Apache DataFusion as the query engine. kubectl contexts are treated as databases (switchable with `USE cluster1;`) and Kubernetes resources are exposed as tables. The `_cluster` column is part of every table's primary key, enabling cross-cluster queries.

## Build Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run all tests
```

## Usage Examples

```bash
# Interactive REPL (default)
k8sql

# Batch mode - single query
k8sql -q "SELECT * FROM pods"
k8sql -q "SELECT name, json_get_str(status, 'phase') as phase FROM pods WHERE namespace = 'kube-system'"

# Multi-cluster queries
k8sql -q "SELECT * FROM pods WHERE _cluster = 'prod'"
k8sql -q "SELECT * FROM pods WHERE _cluster = '*'"  # All clusters

# Batch mode with context and output format
k8sql -c prod-cluster -q "SELECT * FROM deployments" -o json

# Daemon mode (PostgreSQL wire protocol)
k8sql daemon --port 15432
```

## Architecture

```
src/
├── main.rs                    # Entry point, CLI argument parsing, mode dispatch
├── cli/
│   ├── mod.rs
│   ├── args.rs                # Clap argument definitions
│   └── repl.rs                # Rustyline REPL with completion/highlighting
├── datafusion_integration/
│   ├── mod.rs
│   ├── context.rs             # K8sSessionContext - DataFusion session setup
│   ├── provider.rs            # K8sTableProvider - TableProvider implementation
│   ├── convert.rs             # JSON to Arrow RecordBatch conversion
│   └── hooks.rs               # Query hooks for filter extraction
├── kubernetes/
│   ├── mod.rs
│   ├── client.rs              # K8sClientPool - multi-cluster connection caching
│   └── discovery.rs           # Dynamic resource discovery and schema generation
├── sql/
│   └── mod.rs                 # ApiFilters type for filter pushdown
├── output/
│   ├── mod.rs                 # QueryResult type and format dispatch
│   ├── table.rs               # Pretty table output
│   ├── json.rs                # JSON output
│   ├── csv.rs                 # CSV output
│   └── yaml.rs                # YAML output
└── daemon/
    ├── mod.rs
    └── pgwire_server.rs       # PostgreSQL wire protocol server
```

## Key Components

### DataFusion Integration (`src/datafusion_integration/`)

k8sql uses Apache DataFusion as its SQL query engine:

- **K8sSessionContext** (`context.rs`): Wraps DataFusion's SessionContext, registers all K8s resources as tables, and provides JSON functions for querying nested fields.

- **K8sTableProvider** (`provider.rs`): Implements DataFusion's TableProvider trait. Fetches data from Kubernetes API on scan, pushes namespace and cluster filters.

- **JSON to Arrow conversion** (`convert.rs`): Converts K8s JSON resources to Arrow RecordBatches for DataFusion processing.

### Resource Discovery (`src/kubernetes/discovery.rs`)

Dynamic resource discovery at startup:
- Discovers all available resources including CRDs
- Generates consistent schema: `_cluster`, `api_version`, `kind`, metadata fields, `spec`/`status` as JSON
- Special schemas for ConfigMaps, Secrets, Events, and metrics resources
- Core resources (v1, apps, batch, etc.) take priority over conflicting names from other API groups

### K8sClientPool (`src/kubernetes/client.rs`)

Manages connections to multiple Kubernetes clusters:
- Caches clients by context name
- Caches resource registries with TTL
- Supports parallel queries across clusters

## Key Dependencies

- **datafusion**: SQL query engine (Apache Arrow-based)
- **datafusion-functions-json**: JSON querying functions (`json_get_str`, `json_get_int`, etc.)
- **datafusion-postgres**: PostgreSQL wire protocol for daemon mode
- **kube**: Kubernetes client (kube-rs)
- **k8s-openapi**: Kubernetes API type definitions (v1.32)
- **rustyline**: REPL with readline support
- **clap**: CLI argument parsing

## Table Schema

All resources share a consistent schema:

| Column | Type | Description |
|--------|------|-------------|
| `_cluster` | text | Kubernetes context name |
| `api_version` | text | API version (v1, apps/v1, etc.) |
| `kind` | text | Resource kind (Pod, Deployment, etc.) |
| `name` | text | Resource name |
| `namespace` | text | Namespace (null for cluster-scoped) |
| `uid` | text | Unique identifier |
| `created` | timestamp | Creation timestamp |
| `labels` | json | Resource labels |
| `annotations` | json | Resource annotations |
| `spec` | json | Resource specification |
| `status` | json | Resource status |

Special cases:
- **ConfigMaps/Secrets**: `data` column instead of spec/status
- **Events**: `type`, `reason`, `message`, `count`, etc.
- **PodMetrics/NodeMetrics**: `timestamp`, `window`, `containers`/`usage`

## Query Optimization

Currently pushed to K8s API:

| SQL Condition | K8s API Optimization |
|---------------|---------------------|
| `namespace = 'x'` | Uses namespaced API |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster = '*'` | Queries all clusters in parallel |

Handled client-side by DataFusion:
- All JSON field access (`json_get_str(status, 'phase')`)
- LIKE patterns
- Complex expressions
- ORDER BY, LIMIT

## REPL Controls

- **Tab**: Auto-complete keywords/tables/columns
- **↑/↓**: Navigate history
- **Ctrl+D**: Quit
- **\dt**: SHOW TABLES
- **\l**: SHOW DATABASES
- **\d table**: DESCRIBE table
- **\x**: Toggle expanded display
- **\q**: Quit
