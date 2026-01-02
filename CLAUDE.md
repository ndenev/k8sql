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

## Git Commits

- Do NOT add Claude attributions to commit messages (no "Generated with Claude Code", no "Co-Authored-By: Claude")
- Write clear, conventional commit messages focusing on what changed and why
- Always run `cargo fmt` before committing
- Always verify with `cargo clippy` for lint issues before committing

## Code Navigation

- Use the Serena MCP with rust-analyzer for code navigation and symbol lookup
- Prefer symbolic tools (find_symbol, find_referencing_symbols, get_symbols_overview) over grep for Rust code

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
k8sql -q "SELECT name, status->>'phase' as phase FROM pods WHERE namespace = 'kube-system'"

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
│   ├── preprocess.rs          # SQL preprocessing (fixes ->> operator precedence)
│   └── hooks.rs               # Query hooks for filter extraction
├── kubernetes/
│   ├── mod.rs                 # ApiFilters type for filter pushdown
│   ├── cache.rs               # Resource discovery cache persistence
│   ├── client.rs              # K8sClientPool - multi-cluster connection caching
│   ├── discovery.rs           # Dynamic resource discovery and schema generation
│   └── field_selectors.rs     # Field selector registry and types for K8s API pushdown
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

- **K8sSessionContext** (`context.rs`): Wraps DataFusion's SessionContext, registers all K8s resources as tables, provides JSON functions for querying nested fields.

- **K8sTableProvider** (`provider.rs`): Implements DataFusion's TableProvider trait. Fetches data from Kubernetes API on scan, extracts and pushes filters (namespace, cluster, label selectors) to the API.

- **JSON to Arrow conversion** (`convert.rs`): Converts K8s JSON resources to Arrow RecordBatches. Metadata fields use native Arrow types (Timestamp for dates, Int64 for integers); labels/annotations/spec/status are stored as JSON strings.

- **SQL Preprocessing** (`preprocess.rs`): Fixes `->>` operator precedence before DataFusion parsing. Wraps `col->>'key' = 'value'` as `(col->>'key') = 'value'` to work around parser quirks.

### Resource Discovery (`src/kubernetes/discovery.rs`)

Dynamic resource discovery at startup:
- Discovers all available resources including CRDs
- Generates consistent schema: `_cluster`, `api_version`, `kind`, metadata fields, `spec`/`status` as JSON
- Special schemas for ConfigMaps, Secrets, Events, and metrics resources
- Core resources (v1, apps, batch, etc.) take priority over conflicting names from other API groups

#### ColumnDataType Enum

Schema definitions use a type-safe enum for Arrow data types:

```rust
pub enum ColumnDataType {
    Text,       // Arrow Utf8 - strings, JSON blobs
    Timestamp,  // Arrow Timestamp(ms) - creation/deletion timestamps
    Integer,    // Arrow Int64 - generation, count fields
}
```

Helper constructors simplify schema definition:
```rust
ColumnDef::text("name", "metadata.name")
ColumnDef::timestamp("created", "metadata.creationTimestamp")
ColumnDef::integer("generation", "metadata.generation")
ColumnDef::text_raw("_cluster")  // No JSON path (virtual column)
```

### K8sClientPool (`src/kubernetes/client.rs`)

Manages connections to multiple Kubernetes clusters:
- Caches clients by context name
- Caches resource registries with TTL
- Supports parallel queries across clusters

**Discovery Cache (30-minute TTL):**
- Resource discovery (CRDs, schemas, available tables) is cached for 30 minutes
- `USE` commands use cached discovery for instant context switching
- Cache reduces API load and improves performance with large kubeconfigs
- Only discovery metadata is cached - actual resource queries always hit the API
- Force fresh discovery: restart k8sql or use `--refresh-crds` flag

**Retry Logic:**
- Connection and discovery failures retry 3 times with exponential backoff (100ms, 200ms, 300ms)
- Handles intermittent network issues and proxy problems
- All requested contexts must succeed - no partial failures (ensures predictability for scripting)

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

| Column | Arrow Type | Description |
|--------|------------|-------------|
| `_cluster` | Utf8 | Kubernetes context name |
| `api_version` | Utf8 | API version (v1, apps/v1, etc.) |
| `kind` | Utf8 | Resource kind (Pod, Deployment, etc.) |
| `name` | Utf8 | Resource name |
| `namespace` | Utf8 | Namespace (null for cluster-scoped) |
| `uid` | Utf8 | Unique identifier |
| `created` | Timestamp(ms) | Creation timestamp (native, supports date comparisons) |
| `generation` | Int64 | Spec change counter (native, supports numeric comparisons) |
| `labels` | Utf8 (JSON) | JSON object, access with `labels->>'key'` |
| `annotations` | Utf8 (JSON) | JSON object, access with `annotations->>'key'` |
| `spec` | Utf8 (JSON) | Resource specification |
| `status` | Utf8 (JSON) | Resource status |

Special cases:
- **ConfigMaps/Secrets**: `data` column instead of spec/status
- **Events**: `type`, `reason`, `message`, `count`, etc.
- **PodMetrics/NodeMetrics**: `timestamp`, `window`, `containers`/`usage`
- **CustomResourceDefinitions**: `group`, `scope`, `resource_kind`, `plural`, `singular`, `short_names`, `categories` (queryable as `crds` or `customresourcedefinitions`)

## Query Optimization

### Server-side (pushed to K8s API):

| SQL Condition | K8s API Optimization |
|---------------|---------------------|
| `namespace = 'x'` | Uses namespaced API endpoint |
| `labels->>'app' = 'nginx'` | K8s label selector |
| `labels->>'app' = 'x' AND labels->>'env' = 'y'` | Combined label selector: `app=x,env=y` |
| `status->>'phase' = 'Running'` | K8s field selector (pods only) |
| `spec->>'nodeName' = 'node-1'` | K8s field selector (pods only) |
| `type = 'Opaque'` | K8s field selector (secrets only) |
| `name = 'pod-123'` | K8s field selector (`metadata.name`) |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster = '*'` | Queries all clusters in parallel |

**Field Selectors** (`src/kubernetes/field_selectors.rs`):
- Limited to `=` and `!=` operators (K8s restriction)
- Resource-specific support (e.g., `status.phase` for pods, `type` for secrets)
- Hardcoded registry based on K8s API documentation
- Namespace field selector intentionally not used (namespaced API endpoints are more efficient)

Supported resources and fields:
- **All resources**: `metadata.name`
- **Pods**: `spec.nodeName`, `spec.restartPolicy`, `spec.schedulerName`, `spec.serviceAccountName`, `spec.hostNetwork`, `status.phase`, `status.podIP`, `status.nominatedNodeName`
- **Events**: `involvedObject.kind`, `involvedObject.namespace`, `involvedObject.name`, `involvedObject.uid`, `involvedObject.apiVersion`, `involvedObject.resourceVersion`, `involvedObject.fieldPath`, `reason`, `reportingComponent`, `source`, `type`
- **Secrets**: `type`
- **Namespaces**: `status.phase`
- **ReplicaSets**: `status.replicas`
- **Jobs**: `status.successful`
- **Nodes**: `spec.unschedulable`
- **CertificateSigningRequests**: `spec.signerName`

### Client-side (DataFusion):
- Unsupported field selectors (fields not in registry)
- LIKE patterns
- Complex expressions
- ORDER BY, LIMIT, GROUP BY
- Operators not supported by K8s field selectors (IN, NOT IN, etc.)

### JSON Array Access

Use `json_get_array()` with `UNNEST` for array expansion:
```sql
SELECT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') FROM pods
```

### Performance Tips

**Projection Pushdown** (automatic optimization):
- k8sql automatically skips converting unrequested JSON columns
- `SELECT name FROM pods` only processes ~50 bytes per pod (vs ~2KB for full resource)
- `SELECT name, namespace, labels FROM pods` processes ~200 bytes per pod
- Impact: **10-40x reduction** in CPU/memory for selective queries
- No user action needed - optimization happens automatically

**Query Performance Best Practices**:
1. **Select only needed columns**: `SELECT name, namespace FROM pods` is much faster than `SELECT * FROM pods`
2. **Use namespace filters**: `WHERE namespace = 'kube-system'` uses efficient namespaced API endpoint
3. **Use label selectors**: `WHERE labels->>'app' = 'nginx'` pushes to K8s API (reduces network transfer)
4. **Use field selectors**: `WHERE status->>'phase' = 'Running'` for pods pushes to K8s API
5. **Cluster filters**: `WHERE _cluster = 'prod'` avoids querying unnecessary clusters
6. **Combine filters**: Multiple AND conditions on labels/fields push to K8s as combined selectors

**What to Avoid**:
- `SELECT * FROM pods` when you only need a few columns (wastes CPU converting large JSON fields)
- Queries without namespace/cluster filters on large clusters (fetches all resources)
- Client-side filtering (LIKE, complex expressions) when API-pushable filters exist

## REPL Controls

- **Tab**: Auto-complete keywords/tables/columns
- **↑/↓**: Navigate history
- **Ctrl+D**: Quit
- **\dt**: SHOW TABLES (displays: table_name, aliases, group, version, kind, scope, resource_type)
- **\l**: SHOW DATABASES
- **\d table**: DESCRIBE table
- **\x**: Toggle expanded display
- **\q**: Quit
