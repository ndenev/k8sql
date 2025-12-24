use anyhow::{anyhow, Result};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    ConfigMap, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret, Service,
};
use k8s_openapi::api::networking::v1::Ingress;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{api::ListParams, Api, Client, Config};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::sql::ApiFilters;

/// Connection pool for multiple Kubernetes clusters
/// Caches clients by context name for efficient cross-cluster queries
pub struct K8sClientPool {
    kubeconfig: Kubeconfig,
    clients: Arc<RwLock<HashMap<String, Client>>>,
    current_context: Arc<RwLock<String>>,
    default_namespace: Arc<RwLock<String>>,
}

impl K8sClientPool {
    pub async fn new(context: Option<&str>, namespace: &str) -> Result<Self> {
        let kubeconfig = Kubeconfig::read()?;

        let context_name = context
            .map(String::from)
            .or_else(|| kubeconfig.current_context.clone())
            .ok_or_else(|| anyhow!("No context specified and no current context in kubeconfig"))?;

        // Verify context exists
        if !kubeconfig.contexts.iter().any(|c| c.name == context_name) {
            return Err(anyhow!("Context '{}' not found in kubeconfig", context_name));
        }

        let pool = Self {
            kubeconfig,
            clients: Arc::new(RwLock::new(HashMap::new())),
            current_context: Arc::new(RwLock::new(context_name.clone())),
            default_namespace: Arc::new(RwLock::new(namespace.to_string())),
        };

        // Pre-connect to the default context
        pool.get_or_create_client(&context_name).await?;

        Ok(pool)
    }

    /// Get or create a client for the given context
    async fn get_or_create_client(&self, context: &str) -> Result<Client> {
        // Check if we already have a client
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(context) {
                return Ok(client.clone());
            }
        }

        // Verify context exists
        if !self.kubeconfig.contexts.iter().any(|c| c.name == context) {
            return Err(anyhow!("Context '{}' not found in kubeconfig", context));
        }

        // Create new client
        let config = Config::from_custom_kubeconfig(
            self.kubeconfig.clone(),
            &KubeConfigOptions {
                context: Some(context.to_string()),
                ..Default::default()
            },
        )
        .await?;

        let client = Client::try_from(config)?;

        // Cache it
        {
            let mut clients = self.clients.write().await;
            clients.insert(context.to_string(), client.clone());
        }

        Ok(client)
    }

    /// Get client for a specific context, or current context if None
    pub async fn get_client(&self, context: Option<&str>) -> Result<Client> {
        let ctx = match context {
            Some(c) => c.to_string(),
            None => self.current_context.read().await.clone(),
        };
        self.get_or_create_client(&ctx).await
    }

    pub fn list_contexts(&self) -> Result<Vec<String>> {
        Ok(self
            .kubeconfig
            .contexts
            .iter()
            .map(|c| c.name.clone())
            .collect())
    }

    pub async fn current_context(&self) -> Result<String> {
        Ok(self.current_context.read().await.clone())
    }

    pub async fn switch_context(&self, context: &str) -> Result<()> {
        // Verify context exists
        if !self.kubeconfig.contexts.iter().any(|c| c.name == context) {
            return Err(anyhow!("Context '{}' not found", context));
        }

        // Ensure we have a client for this context
        self.get_or_create_client(context).await?;

        // Switch current context
        *self.current_context.write().await = context.to_string();

        Ok(())
    }

    pub async fn default_namespace(&self) -> String {
        self.default_namespace.read().await.clone()
    }

    pub async fn fetch_resources(
        &self,
        table: &str,
        namespace: Option<&str>,
        context: Option<&str>,
        api_filters: &ApiFilters,
    ) -> Result<Vec<serde_json::Value>> {
        let client = self.get_client(context).await?;

        // Build ListParams with any pushed-down filters
        let list_params = self.build_list_params(api_filters);

        match table.to_lowercase().as_str() {
            // Namespaced resources - query specific namespace or all if None
            "pods" | "pod" => self.list_maybe_namespaced::<Pod>(&client, namespace, &list_params).await,
            "services" | "service" | "svc" => self.list_maybe_namespaced::<Service>(&client, namespace, &list_params).await,
            "deployments" | "deployment" | "deploy" => {
                self.list_maybe_namespaced::<Deployment>(&client, namespace, &list_params).await
            }
            "configmaps" | "configmap" | "cm" => {
                self.list_maybe_namespaced::<ConfigMap>(&client, namespace, &list_params).await
            }
            "secrets" | "secret" => self.list_maybe_namespaced::<Secret>(&client, namespace, &list_params).await,
            "ingresses" | "ingress" | "ing" => self.list_maybe_namespaced::<Ingress>(&client, namespace, &list_params).await,
            "jobs" | "job" => self.list_maybe_namespaced::<Job>(&client, namespace, &list_params).await,
            "cronjobs" | "cronjob" | "cj" => self.list_maybe_namespaced::<CronJob>(&client, namespace, &list_params).await,
            "statefulsets" | "statefulset" | "sts" => {
                self.list_maybe_namespaced::<StatefulSet>(&client, namespace, &list_params).await
            }
            "daemonsets" | "daemonset" | "ds" => {
                self.list_maybe_namespaced::<DaemonSet>(&client, namespace, &list_params).await
            }
            "persistentvolumeclaims" | "persistentvolumeclaim" | "pvc" | "pvcs" => {
                self.list_maybe_namespaced::<PersistentVolumeClaim>(&client, namespace, &list_params).await
            }
            // Cluster-scoped resources
            "nodes" | "node" => self.list_cluster::<Node>(&client, &list_params).await,
            "namespaces" | "namespace" | "ns" => self.list_cluster::<Namespace>(&client, &list_params).await,
            "persistentvolumes" | "persistentvolume" | "pv" | "pvs" => {
                self.list_cluster::<PersistentVolume>(&client, &list_params).await
            }
            _ => Err(anyhow!("Unknown table: {}", table)),
        }
    }

    /// Build ListParams from API filters
    fn build_list_params(&self, filters: &ApiFilters) -> ListParams {
        let mut params = ListParams::default();

        if let Some(ref label_sel) = filters.label_selector {
            params = params.labels(label_sel);
        }

        if let Some(ref field_sel) = filters.field_selector {
            params = params.fields(field_sel);
        }

        params
    }

    /// List namespaced resources - if namespace is Some, query that namespace only;
    /// if None, query all namespaces
    async fn list_maybe_namespaced<T>(
        &self,
        client: &Client,
        namespace: Option<&str>,
        list_params: &ListParams,
    ) -> Result<Vec<serde_json::Value>>
    where
        T: kube::Resource<Scope = k8s_openapi::NamespaceResourceScope>
            + Clone
            + DeserializeOwned
            + std::fmt::Debug
            + serde::Serialize,
        <T as kube::Resource>::DynamicType: Default,
    {
        let api: Api<T> = match namespace {
            Some(ns) => Api::namespaced(client.clone(), ns),
            None => Api::all(client.clone()),
        };
        let list = api.list(list_params).await?;
        let values: Vec<serde_json::Value> = list
            .items
            .into_iter()
            .map(|item| serde_json::to_value(item).unwrap_or(serde_json::Value::Null))
            .collect();

        Ok(values)
    }

    async fn list_cluster<T>(
        &self,
        client: &Client,
        list_params: &ListParams,
    ) -> Result<Vec<serde_json::Value>>
    where
        T: kube::Resource<Scope = k8s_openapi::ClusterResourceScope>
            + Clone
            + DeserializeOwned
            + std::fmt::Debug
            + serde::Serialize,
        <T as kube::Resource>::DynamicType: Default,
    {
        let api: Api<T> = Api::all(client.clone());
        let list = api.list(list_params).await?;
        let values: Vec<serde_json::Value> = list
            .items
            .into_iter()
            .map(|item| serde_json::to_value(item).unwrap_or(serde_json::Value::Null))
            .collect();

        Ok(values)
    }
}
