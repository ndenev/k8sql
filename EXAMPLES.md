# k8sql Query Examples

A curated collection of practical SQL queries for Kubernetes clusters. This guide showcases k8sql's capabilities through real-world scenarios across security, operations, troubleshooting, and more.

## Table of Contents

- [Getting Started](#getting-started)
- [Multi-Cluster Operations](#multi-cluster-operations)
- [Operational Health & Monitoring](#operational-health--monitoring)
- [Security & Compliance Auditing](#security--compliance-auditing)
- [Resource Analysis & Capacity Planning](#resource-analysis--capacity-planning)
- [Troubleshooting & Debugging](#troubleshooting--debugging)
- [Advanced Queries & Techniques](#advanced-queries--techniques)
- [Performance Optimization Guide](#performance-optimization-guide)
- [Quick Reference](#quick-reference)
- [Working with Custom Resources (CRDs)](#working-with-custom-resources-crds)
- [Output Formats](#output-formats)

## Prerequisites

- k8sql installed ([installation instructions](README.md#installation))
- Access to one or more Kubernetes clusters
- Clusters configured in your kubeconfig (`kubectl config get-contexts`)

**Tip**: Start k8sql with `k8sql -c <context>` to target a specific cluster, or use `-c '*'` for multi-cluster queries.

---

## Getting Started

**Scenario**: You're new to k8sql and want to explore your cluster.

### 1. List all pods

```sql
SELECT name, namespace
FROM pods
LIMIT 10;
```

Basic query to see pods across all namespaces.

### 2. Show pod details with creation timestamps

```sql
SELECT name, namespace, created, status->>'phase' as phase
FROM pods
ORDER BY created DESC
LIMIT 5;
```

Introduces the `created` timestamp column (native Arrow type) and JSON field extraction.

### 3. Filter by specific namespace

```sql
SELECT name, status->>'phase' as phase
FROM pods
WHERE namespace = 'kube-system';
```

Server-side filtering - only queries the specified namespace via K8s API.

### 4. Count resources by namespace

```sql
SELECT namespace, COUNT(*) as pod_count
FROM pods
GROUP BY namespace
ORDER BY pod_count DESC;
```

Aggregation to understand namespace distribution.

### 5. Find pods with specific labels

```sql
SELECT name, namespace
FROM pods
WHERE labels->>'app' = 'nginx';
```

Server-side label selector pushdown - efficient filtering at API level.

### 6. Multiple label filters

```sql
SELECT name, namespace
FROM pods
WHERE labels->>'app' = 'nginx'
  AND labels->>'env' = 'production';
```

Combined label selectors (sent as `app=nginx,env=production` to K8s API).

### 7. List deployments with replica counts

```sql
SELECT name, namespace,
       json_get_int(spec, 'replicas') as desired,
       json_get_int(status, 'replicas') as current
FROM deployments;
```

Extracting integer values from nested JSON fields.

---

## Multi-Cluster Operations

**Scenario**: You manage multiple Kubernetes clusters and need unified visibility across your fleet.

### 1. Query a specific cluster

```sql
SELECT name, namespace
FROM pods
WHERE _cluster = 'production-us-east';
```

Target a single cluster by name.

### 2. Query all clusters with wildcard

```sql
SELECT _cluster, name, namespace, status->>'phase' as phase
FROM pods
WHERE _cluster = '*'
LIMIT 20;
```

Queries ALL configured clusters in parallel - k8sql's killer feature!

### 3. Compare pod counts across clusters

```sql
SELECT _cluster, COUNT(*) as total_pods
FROM pods
WHERE _cluster = '*'
GROUP BY _cluster
ORDER BY total_pods DESC;
```

Fleet-wide inventory in a single query.

### 4. Find a specific resource across multiple clusters

```sql
SELECT _cluster, name, namespace, status->>'phase' as phase
FROM pods
WHERE _cluster IN ('prod-us', 'prod-eu', 'prod-asia')
  AND name = 'critical-app';
```

Multi-region resource tracking.

### 5. Cross-cluster resource inventory by type

```sql
SELECT _cluster,
       COUNT(DISTINCT CASE WHEN kind = 'Pod' THEN uid END) as pods,
       COUNT(DISTINCT CASE WHEN kind = 'Deployment' THEN uid END) as deployments,
       COUNT(DISTINCT CASE WHEN kind = 'Service' THEN uid END) as services
FROM (
    SELECT _cluster, uid, kind FROM pods WHERE _cluster = '*'
    UNION ALL
    SELECT _cluster, uid, kind FROM deployments WHERE _cluster = '*'
    UNION ALL
    SELECT _cluster, uid, kind FROM services WHERE _cluster = '*'
)
GROUP BY _cluster;
```

Comprehensive fleet inventory across resource types.

### 6. Identify clusters missing critical workloads

```sql
SELECT c.cluster_name
FROM (SELECT DISTINCT _cluster as cluster_name FROM namespaces WHERE _cluster = '*') c
LEFT JOIN (
    SELECT DISTINCT _cluster
    FROM deployments
    WHERE _cluster = '*' AND labels->>'app' = 'monitoring'
) d ON c.cluster_name = d._cluster
WHERE d._cluster IS NULL;
```

Find clusters that don't have a required deployment (e.g., monitoring).

### 7. Cross-cluster failed pods report

```sql
SELECT _cluster, namespace, name,
       status->>'phase' as phase,
       json_get_int(status, 'containerStatuses', 0, 'restartCount') as restarts
FROM pods
WHERE _cluster = '*'
  AND status->>'phase' IN ('Failed', 'Unknown')
ORDER BY restarts DESC;
```

Fleet-wide failure detection.

### 8. Compare ConfigMap configurations across clusters

```sql
SELECT _cluster, namespace, name,
       json_get_str(data, 'config.yaml') as config
FROM configmaps
WHERE _cluster IN ('staging', 'production')
  AND namespace = 'app'
  AND name = 'app-config';
```

Configuration drift detection across environments.

### 9. Global namespace usage summary

```sql
SELECT namespace,
       COUNT(DISTINCT _cluster) as cluster_count,
       SUM(CASE WHEN _cluster = 'prod-us' THEN 1 ELSE 0 END) as prod_us_pods,
       SUM(CASE WHEN _cluster = 'prod-eu' THEN 1 ELSE 0 END) as prod_eu_pods
FROM pods
WHERE _cluster = '*'
  AND namespace NOT LIKE 'kube-%'
GROUP BY namespace
ORDER BY cluster_count DESC;
```

Understand namespace distribution across fleet.

### 10. Cross-cluster container image version drift

```sql
SELECT _cluster,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image,
       COUNT(*) as pod_count
FROM pods
WHERE _cluster = '*'
  AND labels->>'app' = 'web-app'
GROUP BY _cluster, image
ORDER BY _cluster, pod_count DESC;
```

Detect version inconsistencies across clusters for the same application.

---

## Operational Health & Monitoring

**Scenario**: You need to assess cluster health and identify issues quickly.

### 1. Find pods not in Running state

```sql
SELECT name, namespace, status->>'phase' as phase
FROM pods
WHERE status->>'phase' != 'Running';
```

Server-side field selector pushdown for efficiency (uses K8s API `status.phase!=Running`).

### 2. Pods with high restart counts

```sql
SELECT name, namespace,
       json_get_int(status, 'containerStatuses', 0, 'restartCount') as restarts
FROM pods
WHERE json_get_int(status, 'containerStatuses', 0, 'restartCount') > 5
ORDER BY restarts DESC
LIMIT 20;
```

Identify unstable pods experiencing frequent restarts.

### 3. Unhealthy deployments (replica mismatch)

```sql
SELECT name, namespace,
       json_get_int(spec, 'replicas') as desired,
       json_get_int(status, 'readyReplicas') as ready
FROM deployments
WHERE json_get_int(status, 'readyReplicas') < json_get_int(spec, 'replicas')
   OR json_get_int(status, 'readyReplicas') IS NULL;
```

Find deployments not meeting desired replica count.

### 4. Pending pods (scheduling issues)

```sql
SELECT name, namespace, created,
       json_get_str(status, 'conditions', 0, 'message') as reason
FROM pods
WHERE status->>'phase' = 'Pending'
ORDER BY created;
```

Identify pods that can't be scheduled.

### 5. Nodes with high pod count

```sql
SELECT spec->>'nodeName' as node,
       COUNT(*) as pod_count
FROM pods
WHERE spec->>'nodeName' IS NOT NULL
  AND status->>'phase' = 'Running'
GROUP BY spec->>'nodeName'
ORDER BY pod_count DESC;
```

Node capacity monitoring.

### 6. Recent pod failures (last 24 hours)

```sql
SELECT name, namespace, status->>'phase' as phase, created
FROM pods
WHERE status->>'phase' IN ('Failed', 'Unknown')
  AND created > CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY created DESC;
```

Track recent failures for investigation.

### 7. Pods stuck in ImagePullBackOff

```sql
SELECT name, namespace,
       json_get_str(status, 'containerStatuses', 0, 'state', 'waiting', 'reason') as reason,
       json_get_str(status, 'containerStatuses', 0, 'image') as image
FROM pods
WHERE json_get_str(status, 'containerStatuses', 0, 'state', 'waiting', 'reason') = 'ImagePullBackOff';
```

Find image pull failures.

### 8. Pods in CrashLoopBackOff

```sql
SELECT name, namespace,
       json_get_str(status, 'containerStatuses', 0, 'state', 'waiting', 'reason') as reason,
       json_get_int(status, 'containerStatuses', 0, 'restartCount') as restarts
FROM pods
WHERE json_get_str(status, 'containerStatuses', 0, 'state', 'waiting', 'reason') = 'CrashLoopBackOff'
ORDER BY restarts DESC;
```

Detect crashing containers.

### 9. Jobs that haven't completed

```sql
SELECT name, namespace, created,
       json_get_int(status, 'active') as active,
       json_get_int(status, 'failed') as failed
FROM jobs
WHERE json_get_int(status, 'succeeded') IS NULL
  AND created < CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY created;
```

Find stuck or failing batch jobs.

### 10. Nodes marked as unschedulable

```sql
SELECT name,
       json_get_bool(spec, 'unschedulable') as unschedulable,
       json_get_str(status, 'conditions', 0, 'type') as condition_type,
       json_get_str(status, 'conditions', 0, 'status') as condition_status
FROM nodes
WHERE json_get_bool(spec, 'unschedulable') = true;
```

Check for nodes excluded from scheduling.

### 11. StatefulSets with incorrect replica count

```sql
SELECT name, namespace,
       json_get_int(spec, 'replicas') as desired,
       json_get_int(status, 'readyReplicas') as ready,
       json_get_int(status, 'currentReplicas') as current
FROM statefulsets
WHERE json_get_int(status, 'readyReplicas') != json_get_int(spec, 'replicas');
```

Monitor stateful workload health.

### 12. DaemonSet rollout status

```sql
SELECT name, namespace,
       json_get_int(status, 'desiredNumberScheduled') as desired,
       json_get_int(status, 'numberReady') as ready,
       json_get_int(status, 'updatedNumberScheduled') as updated
FROM daemonsets
WHERE json_get_int(status, 'numberReady') < json_get_int(status, 'desiredNumberScheduled');
```

Verify DaemonSet deployment status.

---

## Security & Compliance Auditing

**Scenario**: You need to audit cluster security posture and ensure compliance with organizational policies.

### 1. Pods without resource limits

```sql
SELECT name, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'name') as container
FROM pods
WHERE json_get_json(UNNEST(json_get_array(spec, 'containers')), 'resources', 'limits') IS NULL;
```

Find pods that could consume unlimited resources.

### 2. Containers running as root (UID 0)

```sql
SELECT name, namespace,
       json_get_str(spec, 'containers', 0, 'name') as container,
       json_get_int(spec, 'securityContext', 'runAsUser') as uid
FROM pods
WHERE json_get_int(spec, 'securityContext', 'runAsUser') = 0
   OR json_get_int(spec, 'containers', 0, 'securityContext', 'runAsUser') = 0;
```

Security risk: processes running with root privileges.

### 3. Privileged containers

```sql
SELECT name, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'name') as container,
       json_get_bool(UNNEST(json_get_array(spec, 'containers')), 'securityContext', 'privileged') as privileged
FROM pods
WHERE json_get_bool(UNNEST(json_get_array(spec, 'containers')), 'securityContext', 'privileged') = true;
```

Find containers with elevated privileges.

### 4. Pods with host network access

```sql
SELECT name, namespace,
       json_get_bool(spec, 'hostNetwork') as host_network
FROM pods
WHERE json_get_bool(spec, 'hostNetwork') = true;
```

Pods that can access host network namespace.

### 5. Secrets older than 90 days (rotation audit)

```sql
SELECT name, namespace, type, created,
       CURRENT_TIMESTAMP - created as age
FROM secrets
WHERE created < CURRENT_TIMESTAMP - INTERVAL '90 days'
  AND type != 'kubernetes.io/service-account-token'
ORDER BY created;
```

Identify secrets that should be rotated.

### 6. ServiceAccounts with secrets

```sql
SELECT name, namespace,
       json_length(secrets) as secret_count
FROM serviceaccounts
WHERE json_length(secrets) > 0
ORDER BY secret_count DESC;
```

Audit service account token usage.

### 7. ClusterRoleBindings granting cluster-admin

```sql
SELECT name,
       json_get_str(role_ref, 'name') as role_name,
       json_get_str(UNNEST(json_get_array(subjects)), 'kind') as subject_kind,
       json_get_str(UNNEST(json_get_array(subjects)), 'name') as subject_name
FROM clusterrolebindings
WHERE json_get_str(role_ref, 'name') = 'cluster-admin';
```

Find subjects with cluster-admin privileges.

### 8. Pods with hostPath volumes

```sql
SELECT name, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'volumes')), 'name') as volume_name,
       json_get_str(UNNEST(json_get_array(spec, 'volumes')), 'hostPath', 'path') as host_path
FROM pods
WHERE json_get_str(UNNEST(json_get_array(spec, 'volumes')), 'hostPath', 'path') IS NOT NULL;
```

Volumes mounting host filesystem (security risk).

### 9. Ingresses without TLS

```sql
SELECT name, namespace,
       json_get_array(spec, 'rules') as rules
FROM ingresses
WHERE json_get_array(spec, 'tls') IS NULL
   OR json_length(json_get_array(spec, 'tls')) = 0;
```

Find ingresses serving unencrypted traffic.

### 10. Namespaces without NetworkPolicies

```sql
SELECT n.name as namespace
FROM namespaces n
LEFT JOIN (
    SELECT DISTINCT namespace
    FROM networkpolicies
) np ON n.name = np.namespace
WHERE np.namespace IS NULL
  AND n.name NOT LIKE 'kube-%';
```

Identify namespaces lacking network segmentation.

### 11. Pods using default ServiceAccount

```sql
SELECT name, namespace,
       json_get_str(spec, 'serviceAccountName') as service_account
FROM pods
WHERE json_get_str(spec, 'serviceAccountName') IN ('default', '')
   OR json_get_str(spec, 'serviceAccountName') IS NULL;
```

Pods not using dedicated service accounts.

### 12. Cross-cluster security posture comparison

```sql
SELECT _cluster,
       COUNT(*) as total_pods,
       SUM(CASE WHEN json_get_bool(spec, 'hostNetwork') = true THEN 1 ELSE 0 END) as host_network_pods,
       SUM(CASE WHEN json_get_bool(spec, 'containers', 0, 'securityContext', 'privileged') = true THEN 1 ELSE 0 END) as privileged_pods
FROM pods
WHERE _cluster = '*'
GROUP BY _cluster
ORDER BY (host_network_pods + privileged_pods) DESC;
```

Fleet-wide security metric comparison.

---

## Resource Analysis & Capacity Planning

**Scenario**: You need insights for resource optimization and capacity planning.

### 1. Container image inventory

```sql
SELECT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image,
       COUNT(*) as usage_count
FROM pods
GROUP BY image
ORDER BY usage_count DESC
LIMIT 20;
```

Most commonly used container images.

### 2. Resources per namespace

```sql
SELECT namespace,
       COUNT(CASE WHEN kind = 'Pod' THEN 1 END) as pods,
       COUNT(CASE WHEN kind = 'Deployment' THEN 1 END) as deployments,
       COUNT(CASE WHEN kind = 'Service' THEN 1 END) as services
FROM (
    SELECT namespace, 'Pod' as kind FROM pods
    UNION ALL
    SELECT namespace, 'Deployment' as kind FROM deployments
    UNION ALL
    SELECT namespace, 'Service' as kind FROM services
)
GROUP BY namespace
ORDER BY pods DESC;
```

Namespace resource distribution.

### 3. ConfigMaps by size

```sql
SELECT name, namespace,
       json_length(data) as key_count
FROM configmaps
WHERE json_length(data) > 10
ORDER BY key_count DESC;
```

Identify large configuration objects.

### 4. PersistentVolume capacity summary

```sql
SELECT json_get_str(spec, 'storageClassName') as storage_class,
       COUNT(*) as volume_count,
       SUM(CAST(REPLACE(json_get_str(spec, 'capacity', 'storage'), 'Gi', '') AS INTEGER)) as total_gb
FROM persistentvolumes
GROUP BY storage_class
ORDER BY total_gb DESC;
```

Storage capacity by storage class.

### 5. Services by type

```sql
SELECT json_get_str(spec, 'type') as service_type,
       COUNT(*) as count
FROM services
GROUP BY service_type
ORDER BY count DESC;
```

Service type distribution.

### 6. Resources older than 6 months

```sql
SELECT kind, name, namespace, created,
       CURRENT_TIMESTAMP - created as age
FROM (
    SELECT 'Pod' as kind, name, namespace, created FROM pods
    UNION ALL
    SELECT 'Deployment' as kind, name, namespace, created FROM deployments
    UNION ALL
    SELECT 'Service' as kind, name, namespace, created FROM services
)
WHERE created < CURRENT_TIMESTAMP - INTERVAL '6 months'
ORDER BY created;
```

Potential cleanup candidates.

### 7. Label key usage distribution

```sql
SELECT json_keys(labels) as label_keys,
       COUNT(*) as resource_count
FROM pods
WHERE labels IS NOT NULL
GROUP BY label_keys
ORDER BY resource_count DESC
LIMIT 10;
```

Common label patterns across resources.

### 8. ResourceQuota usage per namespace

```sql
SELECT name, namespace,
       json_get_str(spec, 'hard', 'pods') as hard_pods,
       json_get_str(status, 'used', 'pods') as used_pods
FROM resourcequotas
ORDER BY namespace;
```

Quota monitoring.

### 9. HorizontalPodAutoscaler status

```sql
SELECT name, namespace,
       json_get_int(spec, 'minReplicas') as min_replicas,
       json_get_int(spec, 'maxReplicas') as max_replicas,
       json_get_int(status, 'currentReplicas') as current_replicas,
       json_get_int(status, 'desiredReplicas') as desired_replicas
FROM horizontalpodautoscalers;
```

Autoscaling configuration and status.

### 10. Pod distribution across nodes

```sql
SELECT spec->>'nodeName' as node,
       COUNT(*) as pod_count,
       COUNT(DISTINCT namespace) as namespace_count
FROM pods
WHERE spec->>'nodeName' IS NOT NULL
GROUP BY spec->>'nodeName'
ORDER BY pod_count DESC;
```

Node balancing analysis.

---

## Troubleshooting & Debugging

**Scenario**: You need to diagnose issues and find root causes quickly.

### 1. Find events for a specific pod

```sql
SELECT type, reason, message, first_timestamp, count
FROM events
WHERE involved_object->>'kind' = 'Pod'
  AND involved_object->>'name' = 'my-pod'
  AND involved_object->>'namespace' = 'default'
ORDER BY first_timestamp DESC;
```

Event correlation for pod troubleshooting.

### 2. Warning and error events in the last hour

```sql
SELECT type, reason, involved_object->>'name' as resource, message, first_timestamp
FROM events
WHERE type IN ('Warning', 'Error')
  AND first_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY first_timestamp DESC
LIMIT 50;
```

Recent issues across the cluster.

### 3. Events by reason (pattern identification)

```sql
SELECT reason, type, COUNT(*) as occurrence_count
FROM events
WHERE first_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY reason, type
ORDER BY occurrence_count DESC;
```

Common failure patterns.

### 4. All pods on a specific node

```sql
SELECT name, namespace, status->>'phase' as phase,
       json_get_int(status, 'containerStatuses', 0, 'restartCount') as restarts
FROM pods
WHERE spec->>'nodeName' = 'node-1';
```

Node-specific troubleshooting.

### 5. Pods using a specific container image

```sql
SELECT name, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') as image
FROM pods
WHERE json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image') LIKE '%nginx:1.19%';
```

Find pods affected by a specific image version.

### 6. Failed jobs in the last 7 days

```sql
SELECT name, namespace, created,
       json_get_int(status, 'failed') as failures,
       json_get_str(status, 'conditions', 0, 'reason') as failure_reason
FROM jobs
WHERE json_get_int(status, 'failed') > 0
  AND created > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY failures DESC;
```

Batch job failure analysis.

### 7. Pods with OOMKilled containers

```sql
SELECT name, namespace,
       json_get_str(status, 'containerStatuses', 0, 'name') as container,
       json_get_str(status, 'containerStatuses', 0, 'lastState', 'terminated', 'reason') as last_termination_reason
FROM pods
WHERE json_get_str(status, 'containerStatuses', 0, 'lastState', 'terminated', 'reason') = 'OOMKilled';
```

Memory limit violations.

### 8. Services with no matching pods

```sql
SELECT s.name, s.namespace,
       json_get_str(s.spec, 'selector') as selector
FROM services s
LEFT JOIN pods p ON s.namespace = p.namespace
  AND json_get_str(s.spec, 'selector', 'app') = p.labels->>'app'
WHERE p.name IS NULL
  AND s.namespace != 'kube-system';
```

Service discovery issues.

### 9. PersistentVolumeClaims without bound volumes

```sql
SELECT name, namespace,
       json_get_str(spec, 'storageClassName') as storage_class,
       json_get_str(status, 'phase') as phase
FROM persistentvolumeclaims
WHERE json_get_str(status, 'phase') != 'Bound';
```

Unbound storage claims.

### 10. Pods with volume mount issues

```sql
SELECT p.name, p.namespace,
       e.reason, e.message
FROM pods p
JOIN events e ON e.involved_object->>'name' = p.name
  AND e.involved_object->>'namespace' = p.namespace
WHERE e.reason LIKE '%Volume%'
  AND e.first_timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour';
```

Volume-related errors.

### 11. Container exit codes of crashed pods

```sql
SELECT name, namespace,
       json_get_int(status, 'containerStatuses', 0, 'lastState', 'terminated', 'exitCode') as exit_code,
       json_get_str(status, 'containerStatuses', 0, 'lastState', 'terminated', 'reason') as reason
FROM pods
WHERE json_get_str(status, 'containerStatuses', 0, 'lastState', 'terminated', 'reason') IS NOT NULL;
```

Crash diagnostics.

### 12. Evicted pods

```sql
SELECT name, namespace, created,
       json_get_str(status, 'reason') as eviction_reason,
       json_get_str(status, 'message') as message
FROM pods
WHERE json_get_str(status, 'reason') = 'Evicted'
ORDER BY created DESC;
```

Resource pressure detection.

---

## Advanced Queries & Techniques

**Scenario**: You want to leverage k8sql's advanced SQL capabilities for complex analysis.

### 1. List all container images using UNNEST

```sql
SELECT DISTINCT json_get_str(container, 'image') as image
FROM (
    SELECT UNNEST(json_get_array(spec, 'containers')) as container
    FROM pods
)
ORDER BY image;
```

Array expansion to extract all container images.

### 2. Extract init containers separately

```sql
SELECT name, namespace,
       json_get_str(UNNEST(json_get_array(spec, 'initContainers')), 'name') as init_container,
       json_get_str(UNNEST(json_get_array(spec, 'initContainers')), 'image') as image
FROM pods
WHERE json_get_array(spec, 'initContainers') IS NOT NULL;
```

Analyze init container usage.

### 3. Common Table Expression (CTE) for complex analysis

```sql
WITH pod_resources AS (
    SELECT namespace,
           json_get_str(spec, 'containers', 0, 'resources', 'requests', 'cpu') as cpu_request,
           json_get_str(spec, 'containers', 0, 'resources', 'requests', 'memory') as mem_request
    FROM pods
)
SELECT namespace, COUNT(*) as pod_count,
       COUNT(cpu_request) as pods_with_cpu_request,
       COUNT(mem_request) as pods_with_mem_request
FROM pod_resources
GROUP BY namespace
ORDER BY pod_count DESC;
```

Multi-stage query for resource request analysis.

### 4. Window function - Rank namespaces by pod count

```sql
SELECT namespace,
       COUNT(*) as pod_count,
       RANK() OVER (ORDER BY COUNT(*) DESC) as rank
FROM pods
GROUP BY namespace
ORDER BY rank;
```

Ranking for comparative analysis.

### 5. Count containers per pod using json_length

```sql
SELECT name, namespace,
       json_length(json_get_array(spec, 'containers')) as container_count
FROM pods
ORDER BY container_count DESC
LIMIT 10;
```

Find pods with many containers.

### 6. Extract nested OwnerReferences

```sql
SELECT name, namespace,
       json_get_str(owner_references, 0, 'kind') as owner_kind,
       json_get_str(owner_references, 0, 'name') as owner_name
FROM pods
WHERE json_length(owner_references) > 0;
```

Deep JSON navigation.

### 7. Pattern matching with LIKE

```sql
SELECT name, namespace
FROM pods
WHERE name LIKE 'app-%-prod-%'
  AND namespace LIKE 'team-%';
```

Flexible string matching.

### 8. Categorize resources by age using CASE

```sql
SELECT name, namespace,
       CASE
           WHEN created > CURRENT_TIMESTAMP - INTERVAL '7 days' THEN 'New'
           WHEN created > CURRENT_TIMESTAMP - INTERVAL '30 days' THEN 'Recent'
           WHEN created > CURRENT_TIMESTAMP - INTERVAL '90 days' THEN 'Mature'
           ELSE 'Old'
       END as age_category
FROM pods
ORDER BY created DESC;
```

Conditional categorization.

### 9. Use COALESCE for default values

```sql
SELECT name, namespace,
       COALESCE(json_get_str(spec, 'serviceAccountName'), 'default') as service_account,
       COALESCE(json_get_int(spec, 'terminationGracePeriodSeconds'), 30) as grace_period
FROM pods;
```

Handle NULL values with defaults.

### 10. Subquery - Find pods without matching services

```sql
SELECT name, namespace, labels->>'app' as app_label
FROM pods
WHERE labels->>'app' NOT IN (
    SELECT DISTINCT json_get_str(spec, 'selector', 'app')
    FROM services
    WHERE json_get_str(spec, 'selector', 'app') IS NOT NULL
)
AND labels->>'app' IS NOT NULL;
```

Orphaned resource detection.

### 11. UNION to combine different resource types

```sql
SELECT 'Pod' as type, name, namespace, created FROM pods
UNION ALL
SELECT 'Deployment' as type, name, namespace, created FROM deployments
UNION ALL
SELECT 'Service' as type, name, namespace, created FROM services
ORDER BY created DESC
LIMIT 20;
```

Cross-resource timeline.

### 12. Extract all JSON keys dynamically

```sql
SELECT DISTINCT json_keys(labels) as label_keys
FROM pods
WHERE labels IS NOT NULL
LIMIT 100;
```

Dynamic schema exploration.

---

## Performance Optimization Guide

Understanding how k8sql pushes filters to the Kubernetes API can dramatically improve query performance.

### Server-Side Filters (Pushed to K8s API)

These filters are executed at the API level, reducing data transfer:

| Filter Type | Example | K8s API Translation |
|-------------|---------|---------------------|
| **Namespace** | `WHERE namespace = 'default'` | Namespaced API endpoint |
| **Cluster** | `WHERE _cluster = 'prod'` | Only queries specified cluster |
| **Label Selector** | `WHERE labels->>'app' = 'nginx'` | K8s label selector: `?labelSelector=app=nginx` |
| **Field Selector** | `WHERE status->>'phase' = 'Running'` | K8s field selector: `?fieldSelector=status.phase=Running` |
| **Multiple Labels** | `labels->>'app' = 'x' AND labels->>'env' = 'y'` | Combined: `?labelSelector=app=x,env=y` |
| **Name Filter** | `WHERE name = 'my-pod'` | Field selector: `?fieldSelector=metadata.name=my-pod` |

### Client-Side Filters (Processed by DataFusion)

These filters require fetching data first, then filtering locally:

- `LIKE` patterns: `WHERE name LIKE 'app-%'`
- Comparisons: `WHERE created > '2025-01-01'`
- JSON expressions: `WHERE json_get_int(spec, 'replicas') > 3`
- Complex logic: `WHERE NOT (status->>'phase' = 'Failed')`
- OR expressions: `WHERE labels->>'app' = 'x' OR labels->>'app' = 'y'`

### Field Selectors by Resource Type

Only certain fields support server-side filtering. Here's the complete list:

**Pods** (supports 8 field selectors):
- `metadata.name` (use `name` column)
- `status->>'phase'`
- `spec->>'nodeName'`
- `spec->>'restartPolicy'`
- `spec->>'schedulerName'`
- `spec->>'serviceAccountName'`
- `spec->>'hostNetwork'`
- `status->>'podIP'`
- `status->>'nominatedNodeName'`

**Events** (supports 11 field selectors):
- `metadata.name`
- `reason`
- `type`
- `involvedObject->>'kind'`
- `involvedObject->>'name'`
- `involvedObject->>'namespace'`
- `involvedObject->>'uid'`
- `involvedObject->>'apiVersion'`
- `involvedObject->>'resourceVersion'`
- `involvedObject->>'fieldPath'`
- `source`
- `reportingComponent`

**Secrets**:
- `metadata.name`
- `type`

**Nodes**:
- `metadata.name`
- `spec->>'unschedulable'`

**Namespaces**:
- `metadata.name`
- `status->>'phase'`

**Jobs**:
- `metadata.name`
- `status->>'successful'`

**ReplicaSets**:
- `metadata.name`
- `status->>'replicas'`

**ReplicationControllers**:
- `metadata.name`
- `status->>'replicas'`

**CertificateSigningRequests**:
- `metadata.name`
- `spec->>'signerName'`

### Best Practices

#### ✅ GOOD: Use server-side filters

```sql
-- Efficient: Pushed to API
SELECT name FROM pods WHERE namespace = 'default' AND status->>'phase' = 'Running';
```

#### ❌ AVOID: Client-side only filters when server-side available

```sql
-- Inefficient: Fetches all pods first, then filters
SELECT name FROM pods WHERE name LIKE 'app%';

-- Better: Use exact match if possible
SELECT name FROM pods WHERE name = 'app-deployment-xyz';
```

#### ✅ GOOD: Combine namespace and label selectors

```sql
-- Both pushed to API
SELECT name FROM pods WHERE namespace = 'production' AND labels->>'app' = 'web';
```

#### ❌ AVOID: Mixing server and client filters unnecessarily

```sql
-- Phase filter pushed, but LIKE prevents optimization
SELECT name FROM pods WHERE status->>'phase' = 'Running' AND name LIKE '%app%';
```

#### ✅ GOOD: Use field selectors for supported fields

```sql
-- Efficient: Field selector pushdown
SELECT name FROM events WHERE reason = 'FailedScheduling';
```

#### ❌ AVOID: Using unsupported fields for filtering

```sql
-- Inefficient: Fetches all events, filters client-side
SELECT name FROM events WHERE message LIKE '%error%';
```

### Verify Filter Pushdown with `-v` Flag

Use the verbose flag to see how k8sql optimizes your query:

```bash
k8sql -v -q "SELECT name FROM pods WHERE namespace = 'default' AND labels->>'app' = 'nginx'"
```

Look for log lines like:
```
DEBUG Namespace filter: Single("default")
DEBUG Label selector: app=nginx
DEBUG Field selector: None
```

This shows which filters were pushed to the K8s API.

---

## Quick Reference

### Common One-Liner Patterns

```sql
-- Recent failures
SELECT name, namespace FROM pods WHERE status->>'phase' = 'Failed' AND created > NOW() - INTERVAL '1 hour';

-- Pods per node
SELECT spec->>'nodeName', COUNT(*) FROM pods GROUP BY spec->>'nodeName';

-- Cross-cluster counts
SELECT _cluster, COUNT(*) FROM pods WHERE _cluster = '*' GROUP BY _cluster;

-- Unhealthy deployments
SELECT name FROM deployments WHERE json_get_int(status, 'readyReplicas') < json_get_int(spec, 'replicas');

-- Recent events
SELECT reason, message FROM events ORDER BY first_timestamp DESC LIMIT 10;

-- Namespace resource counts
SELECT namespace, COUNT(*) FROM pods GROUP BY namespace ORDER BY COUNT(*) DESC;

-- Label filter
SELECT name FROM pods WHERE labels->>'env' = 'production';

-- Image inventory
SELECT json_get_str(UNNEST(json_get_array(spec, 'containers')), 'image'), COUNT(*) FROM pods GROUP BY 1;

-- Old resources
SELECT name, created FROM pods WHERE created < NOW() - INTERVAL '30 days';

-- Multi-namespace query
SELECT name, namespace FROM pods WHERE namespace IN ('default', 'kube-system');
```

### JSON Operator Cheat Sheet

| Operator | Usage | Returns | Example |
|----------|-------|---------|---------|
| `->` | Navigate JSON object | JSON | `status->'conditions'` |
| `->>` | Extract text value | String | `status->>'phase'` |
| `json_get_str()` | Get string value | String | `json_get_str(spec, 'nodeName')` |
| `json_get_int()` | Get integer value | Integer | `json_get_int(spec, 'replicas')` |
| `json_get_bool()` | Get boolean value | Boolean | `json_get_bool(spec, 'hostNetwork')` |
| `json_get_array()` | Get array | Array | `json_get_array(spec, 'containers')` |
| `json_get_json()` | Get nested JSON | JSON | `json_get_json(spec, 'template')` |
| `json_length()` | Array/object length | Integer | `json_length(labels)` |
| `json_keys()` | Object keys | Array | `json_keys(annotations)` |
| `UNNEST()` | Expand array | Rows | `UNNEST(json_get_array(spec, 'containers'))` |

### Common Table Aliases

| Full Name | Short Name | Example |
|-----------|------------|---------|
| pods | po | `SELECT * FROM po;` |
| deployments | deploy | `SELECT * FROM deploy;` |
| services | svc | `SELECT * FROM svc;` |
| namespaces | ns | `SELECT * FROM ns;` |
| nodes | no | `SELECT * FROM no;` |
| configmaps | cm | `SELECT * FROM cm;` |
| persistentvolumeclaims | pvc | `SELECT * FROM pvc;` |
| persistentvolumes | pv | `SELECT * FROM pv;` |
| ingresses | ing | `SELECT * FROM ing;` |
| statefulsets | sts | `SELECT * FROM sts;` |
| daemonsets | ds | `SELECT * FROM ds;` |
| replicasets | rs | `SELECT * FROM rs;` |
| cronjobs | cj | `SELECT * FROM cj;` |
| storageclasses | sc | `SELECT * FROM sc;` |
| horizontalpodautoscalers | hpa | `SELECT * FROM hpa;` |

---

## Working with Custom Resources (CRDs)

k8sql automatically discovers Custom Resource Definitions from all API groups.

### Discover Available CRDs

```sql
-- List all tables (includes CRDs)
SHOW TABLES;

-- Describe CRD schema
DESCRIBE prometheusrules;
```

### Example CRD Queries

#### Prometheus CRDs

```sql
-- List ServiceMonitors
SELECT name, namespace FROM servicemonitors;

-- PrometheusRules with specific labels
SELECT name, namespace FROM prometheusrules WHERE labels->>'team' = 'platform';
```

#### Cert-Manager CRDs

```sql
-- Find expiring certificates
SELECT name, namespace,
       json_get_str(status, 'notAfter') as expiry
FROM certificates;

-- CertificateRequests status
SELECT name, namespace,
       json_get_str(status, 'conditions', 0, 'type') as condition
FROM certificaterequests;
```

#### Istio CRDs

```sql
-- VirtualServices by gateway
SELECT name, namespace,
       json_get_array(spec, 'gateways') as gateways
FROM virtualservices;

-- DestinationRules
SELECT name, namespace,
       json_get_str(spec, 'host') as host
FROM destinationrules;
```

### Refresh CRD Cache

CRDs are cached indefinitely for performance. To refresh:

```bash
k8sql --refresh-crds
```

Then your queries will include newly added CRDs.

---

## Output Formats

k8sql supports multiple output formats for different use cases.

### Table (Default)

Human-readable ASCII tables:

```bash
k8sql -q "SELECT name, namespace FROM pods LIMIT 5"
```

```
┌─────────────────┬────────────┐
│ name            │ namespace  │
├─────────────────┼────────────┤
│ coredns-abc123  │ kube-system│
│ nginx-xyz789    │ default    │
└─────────────────┴────────────┘
```

### JSON

Machine-readable for scripting:

```bash
k8sql -o json -q "SELECT name, namespace FROM pods LIMIT 2"
```

```json
[
  {"name": "coredns-abc123", "namespace": "kube-system"},
  {"name": "nginx-xyz789", "namespace": "default"}
]
```

### CSV

Spreadsheet export:

```bash
k8sql -o csv -q "SELECT name, namespace FROM pods LIMIT 2"
```

```csv
name,namespace
coredns-abc123,kube-system
nginx-xyz789,default
```

### YAML

Kubernetes-native format:

```bash
k8sql -o yaml -q "SELECT name, namespace FROM pods LIMIT 2"
```

```yaml
- name: coredns-abc123
  namespace: kube-system
- name: nginx-xyz789
  namespace: default
```

### Suppress Headers for Scripting

```bash
# Pipe pod names to another tool
k8sql --no-headers -o csv -q "SELECT name FROM pods WHERE namespace = 'default'" | xargs kubectl delete pod
```

### Scripting Example

```bash
# Get failing pods and restart them
FAILING_PODS=$(k8sql --no-headers -o csv \
  -q "SELECT name, namespace FROM pods WHERE status->>'phase' = 'Failed'")

echo "$FAILING_PODS" | while IFS=, read name namespace; do
  kubectl delete pod "$name" -n "$namespace"
done
```

---

## Tips & Tricks

1. **Use `-c` for context selection**: `k8sql -c 'prod-*'` queries all production clusters
2. **Combine with jq**: `k8sql -o json -q "..." | jq '.[] | select(.name | startswith("app"))'`
3. **Save common queries**: Create shell aliases for frequent queries
4. **Use EXPLAIN**: `EXPLAIN SELECT ...` to understand query execution
5. **Monitor with watch**: `watch -n 5 "k8sql -q 'SELECT COUNT(*) FROM pods WHERE status->>\"phase\" = \"Running\"'"`
6. **Cross-cluster diff**: Compare resources across clusters with `_cluster IN ('cluster1', 'cluster2')`
7. **Leverage field selectors**: Use supported field selectors for better performance
8. **Use CTEs for complex analysis**: Break down complex queries with WITH clauses

---

For more information, see the [README](README.md) and [CLAUDE.md](CLAUDE.md).
