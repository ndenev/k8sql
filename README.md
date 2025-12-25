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

-- Exclude problematic clusters
SELECT * FROM pods WHERE _cluster NOT IN ('broken-cluster', 'dev-old')

-- Query ALL clusters in your kubeconfig
SELECT * FROM pods WHERE _cluster = '*'
```

The `'*'` wildcard is a special value that expands to all available kubectl contexts, executing parallel queries across all clusters.

All cluster filters (`=`, `IN`, `NOT IN`) are pushed down to the query planner, so only the specified clusters are actually queried.

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
| `json_get_array(json, key, ...)` | Extract as Arrow array (for UNNEST) |
| `json_length(json)` | Get length of array or object |
| `json_keys(json)` | Get keys of JSON object |
| `json_contains(json, key, ...)` | Check if key exists |

The function syntax supports chaining multiple keys and array indices in a single call, making it convenient for deeply nested access.

### Labels and Annotations

Labels and annotations are stored as JSON strings, accessible via `json_get_str()` or convenient dot notation:

```sql
-- Access a specific label
SELECT name, json_get_str(labels, 'app') as app FROM pods

-- Filter by label (pushed to K8s API as label selector)
SELECT * FROM pods WHERE labels.app = 'nginx'

-- Multiple label conditions (combined into single API call)
SELECT * FROM pods WHERE labels.app = 'nginx' AND labels.env = 'prod'

-- Access annotations
SELECT name, json_get_str(annotations, 'kubectl.kubernetes.io/last-applied-configuration') as config
FROM deployments

-- Labels with special characters (dots, slashes)
SELECT * FROM pods WHERE labels.app.kubernetes.io/name = 'cert-manager'
```

The `labels.key = 'value'` syntax is automatically converted to `json_get_str(labels, 'key')` and pushed to the Kubernetes API as a label selector for efficient server-side filtering.

### Working with Arrays (UNNEST)

Use `json_get_array()` with `UNNEST` to expand JSON arrays into rows:

```sql
-- Get all containers from a pod
SELECT name, UNNEST(json_get_array(spec, 'containers')) as container
FROM pods
WHERE namespace = 'kube-system'

-- Get all container images from a pod
SELECT name, json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods

-- Get all unique images across all pods
SELECT DISTINCT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
ORDER BY image

-- Get container names and images together
SELECT
    name as pod,
    json_get_str(UNNEST(json_get_array(spec, 'containers')), 'name') as container,
    json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
WHERE namespace = 'default'

-- Count containers per pod
SELECT name, json_length(json_get_json(spec, 'containers')) as container_count
FROM pods

-- Get all volume mounts
SELECT name,
       json_get_str(UNNEST(json_get_array(spec, 'volumes')), 'name') as volume
FROM pods
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

### Container Image Queries

```sql
-- All images used in a namespace
SELECT DISTINCT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
WHERE namespace = 'kube-system'
ORDER BY image

-- Find pods using a specific image (use CTE to filter on UNNEST results)
WITH container_images AS (
    SELECT _cluster, namespace, name as pod_name,
           json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
    FROM pods
)
SELECT * FROM container_images WHERE image LIKE '%nginx%'

-- Images grouped by namespace
SELECT namespace,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
ORDER BY namespace, image

-- Count pods per image (top 10 most used images)
SELECT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image,
       COUNT(*) as count
FROM pods
GROUP BY image
ORDER BY count DESC
LIMIT 10

-- Find pods with multiple containers
SELECT name, namespace, json_length(json_get_json(spec, 'containers')) as container_count
FROM pods
WHERE json_length(json_get_json(spec, 'containers')) > 1

-- All container names and their images
SELECT name as pod, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'name') as container,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
WHERE namespace = 'default'
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
| `labels` | text (JSON) | Resource labels (access with `labels.key` or `json_get_str(labels, 'key')`) |
| `annotations` | text (JSON) | Resource annotations (access with `annotations.key` or `json_get_str`) |
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
| `namespace = 'x'` | Uses `Api::namespaced()` - only fetches from that namespace |
| `labels.app = 'nginx'` | K8s label selector - server-side filtering |
| `json_get_str(labels, 'app') = 'nginx'` | Same as above (explicit JSON access) |
| `labels.app = 'x' AND labels.env = 'y'` | Combined label selector: `app=x,env=y` |
| `_cluster = 'prod'` | Only queries that cluster |
| `_cluster IN ('a', 'b')` | Only queries specified clusters |
| `_cluster NOT IN ('x')` | Queries all except specified clusters |
| `_cluster = '*'` | Queries all clusters in parallel |
| `namespace LIKE '%'` | Queries all, filters client-side |
| `json_get_str(...)` | Client-side JSON parsing |

**Server-side optimizations** reduce API calls and network traffic by filtering at the Kubernetes API level. Label selectors are especially powerful - the API returns only matching resources.

**Client-side filters** (LIKE patterns, JSON field comparisons) are applied after fetching resources. For large clusters, prefer server-side filters when possible.

Use `-v` flag to see what filters are being pushed down:
```bash
k8sql -v -q "SELECT name FROM pods WHERE labels.app = 'nginx' AND namespace = 'default'"
# Shows: labels=Some("app=nginx"), namespace=Some("default")
```

## REPL Commands

```
SHOW TABLES          - List available tables
SHOW DATABASES       - List kubectl contexts (active ones marked with *)
DESCRIBE <table>     - Show table schema
USE <cluster>        - Switch to cluster(s) - see below
\dt                  - Shortcut for SHOW TABLES
\l                   - Shortcut for SHOW DATABASES
\d <table>           - Shortcut for DESCRIBE
\x                   - Toggle expanded display mode
\q                   - Quit
```

### Multi-Cluster USE

The `USE` command supports multiple clusters and glob patterns:

```sql
-- Single cluster (traditional)
USE prod-us;

-- Multiple clusters (comma-separated)
USE prod-us, prod-eu, staging;

-- Glob patterns
USE prod-*;           -- matches prod-us, prod-eu, prod-asia, etc.
USE *-staging;        -- matches app-staging, db-staging, etc.
USE prod-?;           -- matches prod-1, prod-2, etc. (? = single char)

-- Combine patterns and explicit names
USE prod-*, staging;
```

After `USE`, queries without a `_cluster` filter will run against all active contexts. `SHOW DATABASES` displays all active contexts marked with `*`.

This is useful for:
- Excluding problematic clusters that always timeout
- Working with a subset of your many clusters
- Quickly switching between environment sets (all prod, all staging, etc.)

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
