
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub description: String,
}

pub fn list_tables() -> Vec<(String, String)> {
    vec![
        ("pods".to_string(), "v1/Pod".to_string()),
        ("services".to_string(), "v1/Service".to_string()),
        ("deployments".to_string(), "apps/v1/Deployment".to_string()),
        ("configmaps".to_string(), "v1/ConfigMap".to_string()),
        ("secrets".to_string(), "v1/Secret".to_string()),
        ("nodes".to_string(), "v1/Node".to_string()),
        ("namespaces".to_string(), "v1/Namespace".to_string()),
        ("ingresses".to_string(), "networking.k8s.io/v1/Ingress".to_string()),
        ("jobs".to_string(), "batch/v1/Job".to_string()),
        ("cronjobs".to_string(), "batch/v1/CronJob".to_string()),
        ("statefulsets".to_string(), "apps/v1/StatefulSet".to_string()),
        ("daemonsets".to_string(), "apps/v1/DaemonSet".to_string()),
        ("pvcs".to_string(), "v1/PersistentVolumeClaim".to_string()),
        ("pvs".to_string(), "v1/PersistentVolume".to_string()),
    ]
}

pub fn get_table_schema(table: &str) -> TableSchema {
    let schema = match table.to_lowercase().as_str() {
        "pods" | "pod" => pods_schema(),
        "services" | "service" | "svc" => services_schema(),
        "deployments" | "deployment" | "deploy" => deployments_schema(),
        "configmaps" | "configmap" | "cm" => configmaps_schema(),
        "secrets" | "secret" => secrets_schema(),
        "nodes" | "node" => nodes_schema(),
        "namespaces" | "namespace" | "ns" => namespaces_schema(),
        "ingresses" | "ingress" | "ing" => ingresses_schema(),
        "jobs" | "job" => jobs_schema(),
        "cronjobs" | "cronjob" | "cj" => cronjobs_schema(),
        "statefulsets" | "statefulset" | "sts" => statefulsets_schema(),
        "daemonsets" | "daemonset" | "ds" => daemonsets_schema(),
        "pvcs" | "persistentvolumeclaims" | "pvc" => pvcs_schema(),
        "pvs" | "persistentvolumes" | "pv" => pvs_schema(),
        _ => TableSchema {
            name: table.to_string(),
            columns: vec![ColumnSchema {
                name: "*".to_string(),
                data_type: "json".to_string(),
                description: "Full resource JSON".to_string(),
            }],
        },
    };
    // Add _cluster as first column to all schemas
    with_cluster_column(schema)
}

fn pods_schema() -> TableSchema {
    TableSchema {
        name: "pods".to_string(),
        columns: vec![
            col("name", "string", "Pod name"),
            col("namespace", "string", "Namespace"),
            col("status.phase", "string", "Pod phase (Running, Pending, etc.)"),
            col("status.podIP", "string", "Pod IP address"),
            col("spec.nodeName", "string", "Node the pod is scheduled on"),
            col("status.containerStatuses", "array", "Container status list"),
            col("age", "duration", "Time since creation"),
            col("labels", "map", "Pod labels"),
        ],
    }
}

fn services_schema() -> TableSchema {
    TableSchema {
        name: "services".to_string(),
        columns: vec![
            col("name", "string", "Service name"),
            col("namespace", "string", "Namespace"),
            col("spec.type", "string", "Service type (ClusterIP, NodePort, etc.)"),
            col("spec.clusterIP", "string", "Cluster IP"),
            col("spec.ports", "array", "Service ports"),
            col("spec.selector", "map", "Pod selector"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn deployments_schema() -> TableSchema {
    TableSchema {
        name: "deployments".to_string(),
        columns: vec![
            col("name", "string", "Deployment name"),
            col("namespace", "string", "Namespace"),
            col("spec.replicas", "integer", "Desired replicas"),
            col("status.readyReplicas", "integer", "Ready replicas"),
            col("status.availableReplicas", "integer", "Available replicas"),
            col("spec.strategy.type", "string", "Deployment strategy"),
            col("age", "duration", "Time since creation"),
            col("labels", "map", "Deployment labels"),
        ],
    }
}

fn configmaps_schema() -> TableSchema {
    TableSchema {
        name: "configmaps".to_string(),
        columns: vec![
            col("name", "string", "ConfigMap name"),
            col("namespace", "string", "Namespace"),
            col("data", "map", "ConfigMap data keys"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn secrets_schema() -> TableSchema {
    TableSchema {
        name: "secrets".to_string(),
        columns: vec![
            col("name", "string", "Secret name"),
            col("namespace", "string", "Namespace"),
            col("type", "string", "Secret type"),
            col("data", "map", "Secret data keys (values hidden)"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn nodes_schema() -> TableSchema {
    TableSchema {
        name: "nodes".to_string(),
        columns: vec![
            col("name", "string", "Node name"),
            col("status.conditions", "array", "Node conditions"),
            col("status.nodeInfo.kubeletVersion", "string", "Kubelet version"),
            col("status.nodeInfo.osImage", "string", "OS image"),
            col("status.capacity.cpu", "string", "CPU capacity"),
            col("status.capacity.memory", "string", "Memory capacity"),
            col("age", "duration", "Time since creation"),
            col("labels", "map", "Node labels"),
        ],
    }
}

fn namespaces_schema() -> TableSchema {
    TableSchema {
        name: "namespaces".to_string(),
        columns: vec![
            col("name", "string", "Namespace name"),
            col("status.phase", "string", "Namespace phase"),
            col("age", "duration", "Time since creation"),
            col("labels", "map", "Namespace labels"),
        ],
    }
}

fn ingresses_schema() -> TableSchema {
    TableSchema {
        name: "ingresses".to_string(),
        columns: vec![
            col("name", "string", "Ingress name"),
            col("namespace", "string", "Namespace"),
            col("spec.rules", "array", "Ingress rules"),
            col("spec.tls", "array", "TLS configuration"),
            col("status.loadBalancer", "object", "Load balancer status"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn jobs_schema() -> TableSchema {
    TableSchema {
        name: "jobs".to_string(),
        columns: vec![
            col("name", "string", "Job name"),
            col("namespace", "string", "Namespace"),
            col("status.succeeded", "integer", "Succeeded count"),
            col("status.failed", "integer", "Failed count"),
            col("status.active", "integer", "Active count"),
            col("spec.completions", "integer", "Desired completions"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn cronjobs_schema() -> TableSchema {
    TableSchema {
        name: "cronjobs".to_string(),
        columns: vec![
            col("name", "string", "CronJob name"),
            col("namespace", "string", "Namespace"),
            col("spec.schedule", "string", "Cron schedule"),
            col("status.lastScheduleTime", "timestamp", "Last schedule time"),
            col("spec.suspend", "boolean", "Suspended"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn statefulsets_schema() -> TableSchema {
    TableSchema {
        name: "statefulsets".to_string(),
        columns: vec![
            col("name", "string", "StatefulSet name"),
            col("namespace", "string", "Namespace"),
            col("spec.replicas", "integer", "Desired replicas"),
            col("status.readyReplicas", "integer", "Ready replicas"),
            col("spec.serviceName", "string", "Service name"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn daemonsets_schema() -> TableSchema {
    TableSchema {
        name: "daemonsets".to_string(),
        columns: vec![
            col("name", "string", "DaemonSet name"),
            col("namespace", "string", "Namespace"),
            col("status.desiredNumberScheduled", "integer", "Desired number"),
            col("status.currentNumberScheduled", "integer", "Current number"),
            col("status.numberReady", "integer", "Ready count"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn pvcs_schema() -> TableSchema {
    TableSchema {
        name: "pvcs".to_string(),
        columns: vec![
            col("name", "string", "PVC name"),
            col("namespace", "string", "Namespace"),
            col("status.phase", "string", "PVC phase"),
            col("spec.storageClassName", "string", "Storage class"),
            col("spec.resources.requests.storage", "string", "Requested storage"),
            col("spec.volumeName", "string", "Bound volume"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn pvs_schema() -> TableSchema {
    TableSchema {
        name: "pvs".to_string(),
        columns: vec![
            col("name", "string", "PV name"),
            col("status.phase", "string", "PV phase"),
            col("spec.capacity.storage", "string", "Capacity"),
            col("spec.storageClassName", "string", "Storage class"),
            col("spec.persistentVolumeReclaimPolicy", "string", "Reclaim policy"),
            col("spec.claimRef.name", "string", "Claimed by"),
            col("age", "duration", "Time since creation"),
        ],
    }
}

fn col(name: &str, data_type: &str, description: &str) -> ColumnSchema {
    ColumnSchema {
        name: name.to_string(),
        data_type: data_type.to_string(),
        description: description.to_string(),
    }
}

/// The _cluster column that's added to all tables
pub fn cluster_column() -> ColumnSchema {
    ColumnSchema {
        name: "_cluster".to_string(),
        data_type: "string".to_string(),
        description: "Kubernetes context/cluster name (part of primary key)".to_string(),
    }
}

/// Add _cluster as first column to a schema
pub fn with_cluster_column(mut schema: TableSchema) -> TableSchema {
    schema.columns.insert(0, cluster_column());
    schema
}
