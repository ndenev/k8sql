use anyhow::{Result, anyhow};
use kube::api::DynamicObject;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, Config, api::ListParams};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use super::discovery::{ResourceInfo, ResourceRegistry, discover_resources};
use crate::progress::ProgressHandle;
use crate::sql::ApiFilters;

/// How long to cache discovered resources before auto-refresh
const REGISTRY_TTL: Duration = Duration::from_secs(300); // 5 minutes

/// Timeout for connecting to K8s API
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for reading K8s API responses
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum retry attempts for transient failures
const MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff (doubles each retry)
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// Page size for paginated list requests
/// Smaller pages reduce memory pressure and allow faster initial response
const PAGE_SIZE: u32 = 500;

/// Cached registry with timestamp
struct CachedRegistry {
    registry: ResourceRegistry,
    discovered_at: Instant,
}

impl CachedRegistry {
    fn new(registry: ResourceRegistry) -> Self {
        Self {
            registry,
            discovered_at: Instant::now(),
        }
    }

    fn is_expired(&self) -> bool {
        self.discovered_at.elapsed() > REGISTRY_TTL
    }
}

/// Connection pool for multiple Kubernetes clusters
/// Caches clients and resource registries by context name
pub struct K8sClientPool {
    kubeconfig: Kubeconfig,
    clients: Arc<RwLock<HashMap<String, Client>>>,
    registries: Arc<RwLock<HashMap<String, CachedRegistry>>>,
    /// Current active contexts (supports multi-USE)
    current_contexts: Arc<RwLock<Vec<String>>>,
    /// Progress reporter for query status updates
    progress: ProgressHandle,
}

impl K8sClientPool {
    pub async fn new(context: Option<&str>, _namespace: &str) -> Result<Self> {
        let kubeconfig = Kubeconfig::read()?;

        let context_name = context
            .map(String::from)
            .or_else(|| kubeconfig.current_context.clone())
            .ok_or_else(|| anyhow!("No context specified and no current context in kubeconfig"))?;

        // Verify context exists
        if !kubeconfig.contexts.iter().any(|c| c.name == context_name) {
            return Err(anyhow!(
                "Context '{}' not found in kubeconfig",
                context_name
            ));
        }

        let pool = Self {
            kubeconfig,
            clients: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            current_contexts: Arc::new(RwLock::new(vec![context_name.clone()])),
            progress: crate::progress::create_progress_handle(),
        };

        // Pre-connect to the default context and discover resources
        pool.get_or_create_client(&context_name).await?;
        pool.discover_resources_for_context(&context_name, false)
            .await?;

        Ok(pool)
    }

    /// Discover all available resources for a context
    /// If force is true, always rediscover even if cached
    async fn discover_resources_for_context(&self, context: &str, force: bool) -> Result<()> {
        // Check if already discovered and not expired
        if !force {
            let registries = self.registries.read().await;
            if let Some(cached) = registries.get(context)
                && !cached.is_expired()
            {
                return Ok(());
            }
        }

        let client = self.get_or_create_client(context).await?;

        // Report discovering
        self.progress.discovering(context);
        let start = std::time::Instant::now();

        let registry = discover_resources(&client).await?;
        let table_count = registry.list_tables().len();

        // Report discovery complete
        self.progress
            .discovery_complete(context, table_count, start.elapsed().as_millis() as u64);

        {
            let mut registries = self.registries.write().await;
            registries.insert(context.to_string(), CachedRegistry::new(registry));
        }

        Ok(())
    }

    /// Get the resource registry for the current context
    /// Automatically refreshes if TTL expired
    pub async fn get_registry(&self, context: Option<&str>) -> Result<ResourceRegistry> {
        let ctx = match context {
            Some(c) => c.to_string(),
            None => self
                .current_contexts
                .read()
                .await
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("No current context set"))?,
        };

        // Ensure we have discovered resources for this context (respects TTL)
        self.discover_resources_for_context(&ctx, false).await?;

        let registries = self.registries.read().await;
        registries
            .get(&ctx)
            .map(|c| c.registry.clone())
            .ok_or_else(|| anyhow!("No resource registry for context '{}'", ctx))
    }

    /// Get resource info for a table name
    pub async fn get_resource_info(
        &self,
        table: &str,
        context: Option<&str>,
    ) -> Result<Option<ResourceInfo>> {
        let registry = self.get_registry(context).await?;
        Ok(registry.get(table).cloned())
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

        // Report connecting
        self.progress.connecting(context);
        let start = std::time::Instant::now();

        // Create new client with timeouts
        let mut config = Config::from_custom_kubeconfig(
            self.kubeconfig.clone(),
            &KubeConfigOptions {
                context: Some(context.to_string()),
                ..Default::default()
            },
        )
        .await?;

        // Set timeouts for reliability
        config.connect_timeout = Some(CONNECT_TIMEOUT);
        config.read_timeout = Some(READ_TIMEOUT);

        let client = Client::try_from(config)?;

        // Report connected
        self.progress
            .connected(context, start.elapsed().as_millis() as u64);

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
            None => self
                .current_contexts
                .read()
                .await
                .first()
                .cloned()
                .ok_or_else(|| anyhow!("No current context set"))?,
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

    /// Get the first/primary current context (for backward compatibility)
    pub async fn current_context(&self) -> Result<String> {
        let contexts = self.current_contexts.read().await;
        contexts
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("No current context set"))
    }

    /// Get all current contexts (for multi-USE support)
    pub async fn current_contexts(&self) -> Vec<String> {
        self.current_contexts.read().await.clone()
    }

    /// Get the progress reporter handle for subscribing to updates
    pub fn progress(&self) -> &ProgressHandle {
        &self.progress
    }

    /// Switch to one or more contexts
    /// Supports: "context1" or "context1, context2, context3" or glob patterns like "prod-*"
    pub async fn switch_context(&self, context_spec: &str) -> Result<()> {
        let all_contexts: Vec<String> = self
            .kubeconfig
            .contexts
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // Parse the context specification (comma-separated, with optional glob patterns)
        let mut matched_contexts = Vec::new();

        for part in context_spec.split(',') {
            let pattern = part.trim();
            if pattern.is_empty() {
                continue;
            }

            // Check if it's a glob pattern
            if pattern.contains('*') || pattern.contains('?') {
                // Use simple glob matching
                for ctx in &all_contexts {
                    if Self::glob_match(pattern, ctx) && !matched_contexts.contains(ctx) {
                        matched_contexts.push(ctx.clone());
                    }
                }
            } else {
                // Exact match
                let pattern_str = pattern.to_string();
                if all_contexts.contains(&pattern_str) && !matched_contexts.contains(&pattern_str) {
                    matched_contexts.push(pattern_str);
                } else if !all_contexts.contains(&pattern.to_string()) {
                    return Err(anyhow!("Context '{}' not found", pattern));
                }
            }
        }

        if matched_contexts.is_empty() {
            return Err(anyhow!("No contexts matched pattern '{}'", context_spec));
        }

        // Ensure we have clients and discovered resources for all contexts IN PARALLEL
        let discovery_futures: Vec<_> = matched_contexts
            .iter()
            .map(|ctx| async move {
                self.get_or_create_client(ctx).await?;
                self.discover_resources_for_context(ctx, true).await?;
                Ok::<_, anyhow::Error>(())
            })
            .collect();

        let results = futures::future::join_all(discovery_futures).await;

        // Check for any errors
        for result in results {
            result?;
        }

        // Update current contexts
        *self.current_contexts.write().await = matched_contexts;

        Ok(())
    }

    /// Simple glob pattern matching (supports * and ?)
    /// Uses an efficient iterative algorithm without allocations
    fn glob_match(pattern: &str, text: &str) -> bool {
        let pattern: Vec<char> = pattern.chars().collect();
        let text: Vec<char> = text.chars().collect();

        let mut pi = 0; // pattern index
        let mut ti = 0; // text index
        let mut star_pi = None; // position of last '*' in pattern
        let mut star_ti = 0; // position in text when we saw last '*'

        while ti < text.len() {
            if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
                // Character match or '?' wildcard
                pi += 1;
                ti += 1;
            } else if pi < pattern.len() && pattern[pi] == '*' {
                // '*' wildcard - remember position and try matching zero chars
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
            } else if let Some(sp) = star_pi {
                // Mismatch, but we have a previous '*' - backtrack
                // Try matching one more character with the '*'
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                // Mismatch and no '*' to backtrack to
                return false;
            }
        }

        // Check remaining pattern characters (must all be '*')
        while pi < pattern.len() && pattern[pi] == '*' {
            pi += 1;
        }

        pi == pattern.len()
    }

    /// Fetch resources using dynamic discovery
    /// Works for all resource types: core, extensions, and CRDs
    /// Includes retry logic with exponential backoff for transient failures
    pub async fn fetch_resources(
        &self,
        table: &str,
        namespace: Option<&str>,
        context: Option<&str>,
        api_filters: &ApiFilters,
    ) -> Result<Vec<serde_json::Value>> {
        // Look up resource info from discovery
        let resource_info = self
            .get_resource_info(table, context)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "Unknown table: '{}'. Run SHOW TABLES to see available resources.",
                    table
                )
            })?;

        let client = self.get_client(context).await?;
        let ar = &resource_info.api_resource;
        let list_params = self.build_list_params(api_filters);

        // Create API handle based on resource scope
        let api: Api<DynamicObject> = if resource_info.is_namespaced() {
            match namespace {
                Some(ns) => Api::namespaced_with(client, ns, ar),
                None => Api::all_with(client, ar),
            }
        } else {
            Api::all_with(client, ar)
        };

        // Fetch with retry logic for transient failures
        let list = self
            .list_with_retry(&api, &list_params, table, context)
            .await?;

        // Build apiVersion string (e.g., "v1", "apps/v1", "cert-manager.io/v1")
        let api_version = if resource_info.group.is_empty() {
            resource_info.version.clone()
        } else {
            format!("{}/{}", resource_info.group, resource_info.version)
        };
        let kind = &resource_info.api_resource.kind;

        let values: Vec<serde_json::Value> = list
            .items
            .into_iter()
            .map(|item| {
                let mut value = serde_json::to_value(item).unwrap_or(serde_json::Value::Null);
                // Inject apiVersion and kind (K8s list API doesn't include these per-item)
                if let serde_json::Value::Object(ref mut map) = value {
                    map.insert(
                        "apiVersion".to_string(),
                        serde_json::Value::String(api_version.clone()),
                    );
                    map.insert("kind".to_string(), serde_json::Value::String(kind.clone()));
                }
                value
            })
            .collect();

        Ok(values)
    }

    /// List resources with pagination and retry logic
    /// Uses continue tokens to fetch all pages efficiently
    async fn list_with_retry(
        &self,
        api: &Api<DynamicObject>,
        base_params: &ListParams,
        table: &str,
        context: Option<&str>,
    ) -> Result<kube::api::ObjectList<DynamicObject>> {
        let ctx_name = context.unwrap_or("default");
        let mut all_items: Vec<DynamicObject> = Vec::new();
        let mut continue_token: Option<String> = None;
        let mut page_count = 0u32;

        loop {
            // Build params for this page
            let mut params = base_params.clone().limit(PAGE_SIZE);
            if let Some(ref token) = continue_token {
                params = params.continue_token(token);
            }

            // Fetch with retry
            let list = self
                .list_page_with_retry(api, &params, table, ctx_name)
                .await?;

            let items_count = list.items.len();
            all_items.extend(list.items);
            page_count += 1;

            // Check for more pages
            match list.metadata.continue_ {
                Some(token) if !token.is_empty() => {
                    debug!(
                        table = %table,
                        context = %ctx_name,
                        page = page_count,
                        items_this_page = items_count,
                        total_so_far = all_items.len(),
                        "Fetched page, continuing"
                    );
                    continue_token = Some(token);
                }
                _ => break,
            }
        }

        if page_count > 1 {
            debug!(
                table = %table,
                context = %ctx_name,
                pages = page_count,
                total_items = all_items.len(),
                "Pagination complete"
            );
        }

        // Construct a combined result
        Ok(kube::api::ObjectList {
            metadata: kube::api::ListMeta::default(),
            items: all_items,
            types: Default::default(),
        })
    }

    /// Fetch a single page with retry logic
    async fn list_page_with_retry(
        &self,
        api: &Api<DynamicObject>,
        params: &ListParams,
        table: &str,
        ctx_name: &str,
    ) -> Result<kube::api::ObjectList<DynamicObject>> {
        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            match api.list(params).await {
                Ok(list) => return Ok(list),
                Err(e) => {
                    if Self::is_retryable_error(&e) {
                        let delay = RETRY_BASE_DELAY * 2u32.pow(attempt);
                        warn!(
                            table = %table,
                            context = %ctx_name,
                            attempt = attempt + 1,
                            max_attempts = MAX_RETRIES,
                            delay_ms = delay.as_millis(),
                            error = %e,
                            "Retryable error, backing off"
                        );
                        tokio::time::sleep(delay).await;
                        last_error = Some(e);
                    } else {
                        debug!(
                            table = %table,
                            context = %ctx_name,
                            error = %e,
                            "Non-retryable error"
                        );
                        return Err(anyhow!("K8s API error: {}", e));
                    }
                }
            }
        }

        Err(anyhow!(
            "Failed after {} retries: {}",
            MAX_RETRIES,
            last_error.map(|e| e.to_string()).unwrap_or_default()
        ))
    }

    /// Check if an error is retryable (transient failures)
    fn is_retryable_error(err: &kube::Error) -> bool {
        match err {
            // Network/connection errors are retryable
            kube::Error::HyperError(_) => true,
            // API errors: retry on 429 (rate limit), 503 (unavailable), 504 (timeout)
            kube::Error::Api(api_err) => {
                matches!(api_err.code, 429 | 503 | 504)
            }
            // Timeout errors are retryable
            kube::Error::InferConfig(_) => false,
            kube::Error::Discovery(_) => false,
            _ => false,
        }
    }

    /// Build ListParams from API filters (label selectors, field selectors)
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
}
