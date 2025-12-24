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

k8sql exposes the Kubernetes API as a SQL-compatible database. kubectl contexts are treated as databases (switchable with `USE cluster1;`) and Kubernetes resources are exposed as tables. The `_cluster` column is part of every table's primary key, enabling cross-cluster queries.

## Build Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo run                # Run in interactive REPL mode
cargo run -- -q "..."    # Run batch query
cargo test               # Run all tests
```

## Usage Examples

```bash
# Interactive REPL (default)
k8sql

# Batch mode - single query
k8sql -q "SELECT * FROM pods"
k8sql -q "SELECT name, status.phase FROM pods WHERE namespace = 'kube-system'"

# Multi-cluster queries
k8sql -q "SELECT * FROM pods WHERE _cluster = 'prod'"
k8sql -q "SELECT * FROM pods WHERE _cluster = '*'"  # All clusters

# Batch mode with context and output format
k8sql -c prod-cluster -q "SELECT * FROM deployments" -o json

# Execute queries from file
k8sql -f queries.sql

# Daemon mode (PostgreSQL wire protocol)
k8sql daemon --port 15432
```

## Architecture

```
src/
├── main.rs              # Entry point, CLI argument parsing, mode dispatch
├── cli/
│   ├── mod.rs
│   ├── args.rs          # Clap argument definitions
│   └── repl.rs          # Rustyline REPL with completion/highlighting
├── sql/
│   ├── mod.rs
│   ├── parser.rs        # SQL parsing with sqlparser (PostgreSQL dialect)
│   ├── ast.rs           # Internal query representation (TableRef, ColumnRef, etc.)
│   ├── planner.rs       # Query planner - optimizes predicates for K8s API
│   └── executor.rs      # Query execution using QueryPlan
├── kubernetes/
│   ├── mod.rs
│   ├── client.rs        # K8sClientPool - multi-cluster connection caching
│   └── discovery.rs     # Dynamic resource discovery and schema generation
├── output/
│   └── mod.rs           # QueryResult type and format dispatch (table/json/csv/yaml)
└── daemon/
    ├── mod.rs
    └── pgwire_server.rs # PostgreSQL wire protocol server
```

## Key Components

### Query Planner (`src/sql/planner.rs`)

The query planner analyzes WHERE clauses and determines:
- **ClusterScope**: Which clusters to query (Current, Specific, All)
- **NamespaceScope**: Which namespaces (All or Specific)
- **ApiFilters**: Label/field selectors to push to K8s API
- **ClientFilters**: Conditions to apply client-side after fetch

```rust
pub struct QueryPlan {
    pub cluster_scope: ClusterScope,
    pub namespace_scope: NamespaceScope,
    pub api_filters: ApiFilters,      // Pushed to K8s API
    pub client_filters: ClientFilters, // Applied in executor
    pub table: String,
}
```

### K8sClientPool (`src/kubernetes/client.rs`)

Manages connections to multiple Kubernetes clusters:
- Caches clients by context name
- Supports parallel queries across clusters
- Passes ApiFilters (label/field selectors) to ListParams

## Key Dependencies

- **sqlparser**: SQL parsing with PostgreSQL dialect
- **kube**: Kubernetes client (kube-rs)
- **k8s-openapi**: Kubernetes API type definitions (v1.32)
- **pgwire**: PostgreSQL wire protocol (for daemon mode)
- **rustyline**: REPL with readline support
- **indicatif**: Progress spinners
- **comfy-table**: Pretty table output
- **console**: Terminal colors

## Supported Resources (Tables)

| Table | Aliases | Scope | Primary Key |
|-------|---------|-------|-------------|
| pods | pod | Namespace | (_cluster, namespace, name) |
| services | service, svc | Namespace | (_cluster, namespace, name) |
| deployments | deployment, deploy | Namespace | (_cluster, namespace, name) |
| configmaps | configmap, cm | Namespace | (_cluster, namespace, name) |
| secrets | secret | Namespace | (_cluster, namespace, name) |
| ingresses | ingress, ing | Namespace | (_cluster, namespace, name) |
| jobs | job | Namespace | (_cluster, namespace, name) |
| cronjobs | cronjob, cj | Namespace | (_cluster, namespace, name) |
| statefulsets | statefulset, sts | Namespace | (_cluster, namespace, name) |
| daemonsets | daemonset, ds | Namespace | (_cluster, namespace, name) |
| pvcs | pvc, persistentvolumeclaim | Namespace | (_cluster, namespace, name) |
| nodes | node | Cluster | (_cluster, name) |
| namespaces | namespace, ns | Cluster | (_cluster, name) |
| pvs | pv, persistentvolume | Cluster | (_cluster, name) |

## Query Optimization

The planner pushes these predicates to the K8s API:

| SQL Condition | K8s API Optimization |
|---------------|---------------------|
| `namespace = 'x'` | `Api::namespaced("x")` |
| `labels.app = 'nginx'` | `ListParams::labels("app=nginx")` |
| `status.phase = 'Running'` | `ListParams::fields("status.phase=Running")` |
| `_cluster = 'prod'` | Only query that cluster |

These remain client-side:
- `namespace LIKE '%'`
- `name LIKE 'foo%'`
- Complex expressions

## Development Status

See PLAN.md for detailed roadmap. Current status:
- Phase 1: Core SQL Engine ✓
- Phase 2: Interactive REPL ✓
- Phase 3: Batch Mode ✓
- Phase 4: Multi-cluster support ✓
- Phase 5: Query Planner ✓
- Phase 6: Daemon Mode ✓ (simple query protocol)
- Dynamic Resource Discovery ✓ (CRDs auto-discovered)

## REPL Controls

- **Enter**: Execute query
- **Ctrl+D**: Quit
- **Ctrl+C**: Cancel current input
- **Tab**: Auto-complete keywords/tables/columns
- **↑/↓**: Navigate history
- **help**: Show available commands
- **\dt**: SHOW TABLES
- **\l**: SHOW DATABASES
- **\d table**: DESCRIBE table
- **\x**: Toggle expanded display (for viewing long field values)
- **\q**: Quit
