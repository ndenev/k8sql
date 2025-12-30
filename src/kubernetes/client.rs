use anyhow::{Context, Result, anyhow};
use async_stream::stream;
use futures::Stream;
use kube::api::DynamicObject;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, Config, api::ListParams};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};

use super::ApiFilters;
use super::cache::{CachedResourceInfo, ResourceCache};
use super::discovery::{ResourceInfo, ResourceRegistry};
use crate::progress::ProgressHandle;

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

/// Check if a kube error is retryable (transient failures)
pub fn is_retryable_error(err: &kube::Error) -> bool {
    match err {
        // Network/connection errors are retryable
        kube::Error::HyperError(_) => true,
        // API errors: retry on transient HTTP status codes
        // 408 = Request Timeout, 429 = Rate Limit, 500 = Internal Server Error,
        // 502 = Bad Gateway, 503 = Service Unavailable, 504 = Gateway Timeout
        kube::Error::Api(api_err) => {
            matches!(api_err.code, 408 | 429 | 500 | 502 | 503 | 504)
        }
        kube::Error::InferConfig(_) => false,
        kube::Error::Discovery(_) => false,
        _ => false,
    }
}

/// Execute an async operation with retry logic for transient failures
pub async fn with_retry<T, F, Fut>(operation_name: &str, f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, kube::Error>>,
{
    let mut last_error = None;

    for attempt in 0..MAX_RETRIES {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if is_retryable_error(&e) && attempt < MAX_RETRIES - 1 {
                    let delay = RETRY_BASE_DELAY * 2u32.pow(attempt);
                    warn!(
                        "{} failed (attempt {}/{}): {}, retrying in {:?}",
                        operation_name,
                        attempt + 1,
                        MAX_RETRIES,
                        e,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    last_error = Some(e);
                } else {
                    return Err(e.into());
                }
            }
        }
    }

    Err(anyhow!(
        "{} failed after {} retries: {}",
        operation_name,
        MAX_RETRIES,
        last_error.map(|e| e.to_string()).unwrap_or_default()
    ))
}

/// Page size for paginated list requests
/// Smaller pages reduce memory pressure and allow faster initial response
const PAGE_SIZE: u32 = 500;

/// Check if a context spec contains patterns (*, ?) or multiple contexts (comma-separated)
pub fn is_multi_or_pattern_spec(spec: Option<&str>) -> bool {
    spec.map(|s| s.contains(',') || s.contains('*') || s.contains('?'))
        .unwrap_or(false)
}

/// Extract the first concrete (non-pattern) context from a spec
/// Returns the spec itself if it's a single concrete context
pub fn extract_initial_context(spec: Option<&str>) -> Option<&str> {
    if is_multi_or_pattern_spec(spec) {
        spec.and_then(|s| {
            s.split(',')
                .map(str::trim)
                .find(|c| !c.contains('*') && !c.contains('?'))
        })
    } else {
        spec
    }
}

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
    /// Local disk cache for CRD discovery results
    resource_cache: ResourceCache,
    /// Cache hit counter for diagnostics
    cache_hits: AtomicUsize,
    /// Cache miss counter for diagnostics
    cache_misses: AtomicUsize,
}

impl K8sClientPool {
    /// Create a minimal client pool for unit tests (no kubeconfig required)
    #[cfg(test)]
    pub fn new_for_test(progress: crate::progress::ProgressHandle) -> Self {
        Self {
            kubeconfig: Kubeconfig::default(),
            clients: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            current_contexts: Arc::new(RwLock::new(vec!["test-context".to_string()])),
            progress,
            resource_cache: ResourceCache::new().expect("Failed to create test cache"),
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
        }
    }

    /// Create a new client pool without connecting (fast, no I/O)
    /// Call `initialize()` after subscribing to progress events
    pub fn new(context: Option<&str>) -> Result<Self> {
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

        Ok(Self {
            kubeconfig,
            clients: Arc::new(RwLock::new(HashMap::new())),
            registries: Arc::new(RwLock::new(HashMap::new())),
            current_contexts: Arc::new(RwLock::new(vec![context_name])),
            progress: crate::progress::create_progress_handle(),
            resource_cache: ResourceCache::new()?,
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
        })
    }

    /// Initialize the pool by connecting to the default context and discovering resources
    /// Subscribe to progress() before calling this to receive status updates
    pub async fn initialize(&self) -> Result<()> {
        let context_name = self.current_context().await?;
        self.get_or_create_client(&context_name).await?;
        self.discover_resources_for_context(&context_name, false)
            .await?;
        Ok(())
    }

    /// Create and initialize a pool with a context spec that supports globs and comma-separated lists
    /// Examples: "prod", "prod-*", "prod,staging", "*", or None for current context
    pub async fn with_context_spec(context_spec: Option<&str>) -> Result<Arc<Self>> {
        let initial_context = extract_initial_context(context_spec);

        let pool = Arc::new(Self::new(initial_context)?);
        pool.initialize().await?;

        // If context spec has multiple contexts or patterns, switch to all of them
        if is_multi_or_pattern_spec(context_spec)
            && let Some(spec) = context_spec
        {
            pool.switch_context(spec, false).await?;
        }

        Ok(pool)
    }

    /// Discover all available resources for a context
    /// If force is true, always rediscover CRDs even if cached
    ///
    /// Strategy:
    /// 1. Core resources: Always instant from k8s-openapi (no I/O)
    /// 2. CRDs: Try fingerprint-based cache lookup first
    ///    a. If cluster has cached fingerprint, load CRDs by that fingerprint (no API calls)
    ///    b. If fingerprint cache hit from another cluster, reuse it (2 API calls for fingerprint)
    ///    c. Otherwise, run parallel discovery and cache by fingerprint
    async fn discover_resources_for_context(&self, context: &str, force: bool) -> Result<()> {
        let start = std::time::Instant::now();

        // Check if already in memory and not expired
        if !force {
            let registries = self.registries.read().await;
            if let Some(cached) = registries.get(context)
                && !cached.is_expired()
            {
                // Emit progress events for cached registry (consistent UX)
                self.progress.discovering(context);
                let table_count = cached.registry.list_tables().len();
                self.progress.discovery_complete(
                    context,
                    table_count,
                    start.elapsed().as_millis() as u64,
                );
                return Ok(());
            }
        }

        // Ensure client is ready (validates connection)
        let client = self.get_or_create_client(context).await?;

        // Report discovering
        self.progress.discovering(context);

        // Step 1: Build core registry (instant, no I/O)
        let mut registry = super::discovery::build_core_registry();
        let core_count = registry.list_tables().len();

        // Step 2: Load CRDs with per-API-group caching
        let crd_count = if !force {
            self.load_crds_with_cache(context, &client, &mut registry)
                .await?
        } else {
            // Force refresh - discover all CRDs from cluster
            self.discover_all_crds(context, &client, &mut registry)
                .await?
        };

        let table_count = core_count + crd_count;

        // Report discovery complete
        self.progress
            .discovery_complete(context, table_count, start.elapsed().as_millis() as u64);

        {
            let mut registries = self.registries.write().await;
            registries.insert(context.to_string(), CachedRegistry::new(registry));
        }

        Ok(())
    }

    /// Load CRDs using per-API-group caching
    /// Returns the number of CRDs loaded
    ///
    /// Strategy:
    /// 1. Try to load saved cluster groups from cache (no API call)
    /// 2. If cache is fresh and complete, use it directly
    /// 3. Otherwise, call list_api_groups() to get current groups
    /// 4. Load cached groups, discover only missing groups in parallel
    async fn load_crds_with_cache(
        &self,
        context: &str,
        client: &Client,
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        // Step 1: Try to load from cache first (no API call)
        if let Some(cached_crd_groups) = self.resource_cache.load_cluster_groups(context) {
            let (cached_groups, missing_groups) =
                self.resource_cache.check_groups(&cached_crd_groups);

            // If all groups are cached, load directly without any API calls
            if missing_groups.is_empty() {
                let mut count = 0;
                for cached_group in &cached_groups {
                    for resource in &cached_group.resources {
                        registry.add(resource.clone().into());
                        count += 1;
                    }
                }
                info!(
                    context = %context,
                    cached = cached_groups.len(),
                    "Loaded all CRDs from cache (no API calls)"
                );
                return Ok(count);
            }
            // Some groups missing - fall through to API call
        }

        // Step 2: Get CRD API groups from cluster (single API call)
        let crd_groups = super::discovery::get_crd_api_groups(client).await?;

        if crd_groups.is_empty() {
            return Ok(0);
        }

        // Step 3: Check which groups are cached vs missing
        let (cached_groups, missing_groups) = self.resource_cache.check_groups(&crd_groups);

        let cached_count = cached_groups.len();
        let missing_count = missing_groups.len();

        // Step 3: Load cached groups into registry
        for cached_group in &cached_groups {
            for resource in &cached_group.resources {
                registry.add(resource.clone().into());
            }
        }

        // Step 4: Discover missing groups in parallel (if any)
        let discovered_count = if !missing_groups.is_empty() {
            info!(
                context = %context,
                cached = cached_count,
                missing = missing_count,
                "Loading CRDs (cached: {}, discovering: {})",
                cached_count,
                missing_count
            );

            self.process_discovered_groups(client, context, &missing_groups, registry)
                .await?
        } else {
            info!(
                context = %context,
                cached = cached_count,
                "Loaded all CRDs from cache"
            );
            0
        };

        // Step 5: Save cluster groups list for next time
        if let Err(e) = self
            .resource_cache
            .save_cluster_groups(context, &crd_groups)
        {
            warn!(context = %context, error = %e, "Failed to save cluster groups");
        }

        // Total CRD count
        let crd_count = cached_groups
            .iter()
            .map(|g| g.resources.len())
            .sum::<usize>()
            + discovered_count;

        Ok(crd_count)
    }

    /// Force rediscover all CRDs from the cluster
    /// Gets fresh list of API groups but uses cached group data when available
    async fn discover_all_crds(
        &self,
        context: &str,
        client: &Client,
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        // Get CRD API groups only (single API call - skips core resources)
        let crd_groups = super::discovery::get_crd_api_groups(client).await?;

        if crd_groups.is_empty() {
            return Ok(0);
        }

        // Check which groups are cached vs missing (shared cache across clusters)
        let (cached_groups, missing_groups) = self.resource_cache.check_groups(&crd_groups);

        let cached_count = cached_groups.len();
        let missing_count = missing_groups.len();

        // Load cached groups into registry
        let mut crd_count = 0;
        for cached_group in &cached_groups {
            for resource in &cached_group.resources {
                registry.add(resource.clone().into());
                crd_count += 1;
            }
        }

        // Discover only missing groups in parallel (if any)
        if !missing_groups.is_empty() {
            info!(
                context = %context,
                cached = cached_count,
                missing = missing_count,
                "Force refresh: loading CRDs (cached: {}, discovering: {})",
                cached_count,
                missing_count
            );

            crd_count += self
                .process_discovered_groups(client, context, &missing_groups, registry)
                .await?;
        } else {
            info!(
                context = %context,
                cached = cached_count,
                "Force refresh: all CRDs loaded from shared cache"
            );
        }

        // Save cluster groups list
        if let Err(e) = self
            .resource_cache
            .save_cluster_groups(context, &crd_groups)
        {
            warn!(context = %context, error = %e, "Failed to save cluster groups");
        }

        Ok(crd_count)
    }

    /// Process newly discovered API groups: fetch schemas, cache, and add to registry
    ///
    /// This helper consolidates the common pattern of:
    /// 1. Discovering groups from the API
    /// 2. Fetching CRD schemas from OpenAPI definitions
    /// 3. Saving to the local cache
    /// 4. Adding to the in-memory registry
    async fn process_discovered_groups(
        &self,
        client: &Client,
        context: &str,
        groups: &[(String, String)],
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        let mut discovered = super::discovery::discover_groups(client, groups).await?;

        // Fetch CRD schemas for newly discovered resources
        for resources in discovered.values_mut() {
            if let Err(e) = super::discovery::fetch_crd_schemas(client, resources).await {
                warn!(context = %context, error = %e, "Failed to fetch some CRD schemas");
            }
        }

        // Save and add discovered groups
        let mut count = 0;
        for ((group, version), resources) in &discovered {
            // Convert to cached format
            let cached_resources: Vec<CachedResourceInfo> =
                resources.iter().map(CachedResourceInfo::from).collect();

            // Save to cache
            if let Err(e) = self
                .resource_cache
                .save_group(group, version, &cached_resources)
            {
                warn!(
                    context = %context,
                    group = %group,
                    error = %e,
                    "Failed to cache API group"
                );
            }

            // Add to registry
            for info in resources {
                registry.add(info.clone());
                count += 1;
            }
        }

        Ok(count)
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
        // Always emit connecting event for progress indication
        self.progress.connecting(context);
        let start = std::time::Instant::now();

        // Check if we already have a client
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(context) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);
                // Emit connected event even for cached client
                self.progress
                    .connected(context, start.elapsed().as_millis() as u64);
                return Ok(client.clone());
            }
        }
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Verify context exists
        if !self.kubeconfig.contexts.iter().any(|c| c.name == context) {
            return Err(anyhow!("Context '{}' not found in kubeconfig", context));
        }

        // Create new client with timeouts
        let mut config = Config::from_custom_kubeconfig(
            self.kubeconfig.clone(),
            &KubeConfigOptions {
                context: Some(context.to_string()),
                ..Default::default()
            },
        )
        .await
        .with_context(|| format!("Failed to load kubeconfig for context '{}'", context))?;

        // Set timeouts for reliability
        config.connect_timeout = Some(CONNECT_TIMEOUT);
        config.read_timeout = Some(READ_TIMEOUT);

        let client = Client::try_from(config)
            .with_context(|| format!("Failed to create client for context '{}'", context))?;

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

    /// Force refresh of resource tables (rediscover CRDs from cluster)
    /// This clears the local cache and rediscovers all resources
    pub async fn refresh_tables(&self) -> Result<usize> {
        let contexts = self.current_contexts().await;

        // Discover all contexts in parallel
        let discovery_futures: Vec<_> = contexts
            .iter()
            .map(|ctx| {
                let ctx = ctx.clone();
                async move {
                    self.discover_resources_for_context(&ctx, true)
                        .await
                        .with_context(|| format!("Failed to refresh resources for '{}'", ctx))
                }
            })
            .collect();

        let results = futures::future::join_all(discovery_futures).await;

        // Check for errors
        for result in &results {
            if let Err(e) = result {
                return Err(anyhow!("{}", e));
            }
        }

        // Count total tables
        let registries = self.registries.read().await;
        let total_tables: usize = contexts
            .iter()
            .filter_map(|ctx| registries.get(ctx))
            .map(|cached| cached.registry.list_tables().len())
            .sum();

        Ok(total_tables)
    }

    /// Switch to one or more contexts
    /// Supports: "context1" or "context1, context2, context3" or glob patterns like "prod-*"
    /// If `force_refresh` is true, bypasses cache and does full discovery (use for explicit USE commands)
    /// If false, uses cached discovery results when available (use for startup restore)
    pub async fn switch_context(&self, context_spec: &str, force_refresh: bool) -> Result<()> {
        let all_contexts: Vec<String> = self
            .kubeconfig
            .contexts
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // Resolve context specification to concrete context names
        let matched_contexts =
            super::context_matcher::ContextMatcher::new(&all_contexts).resolve(context_spec)?;

        // Ensure we have clients and discovered resources for all contexts IN PARALLEL
        let discovery_futures: Vec<_> = matched_contexts
            .iter()
            .map(|ctx| {
                let ctx = ctx.clone();
                async move {
                    self.get_or_create_client(&ctx)
                        .await
                        .with_context(|| format!("Failed to connect to cluster '{}'", ctx))?;
                    self.discover_resources_for_context(&ctx, force_refresh)
                        .await
                        .with_context(|| {
                            format!("Failed to discover resources for cluster '{}'", ctx)
                        })?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .collect();

        let results = futures::future::join_all(discovery_futures).await;

        // Check for any errors - collect all failures
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            // Return first error (with context info now included)
            return Err(errors.into_iter().next().unwrap());
        }

        // Update current contexts
        *self.current_contexts.write().await = matched_contexts;

        Ok(())
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
                    if is_retryable_error(&e) {
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

    /// Build ListParams from API filters (label selectors, field selectors)
    fn build_list_params(&self, filters: &ApiFilters) -> ListParams {
        let mut params = ListParams::default();

        if let Some(ref label_sel) = filters.label_selector {
            params = params.labels(label_sel);
        }

        if let Some(ref field_sel) = filters.field_selector {
            params = params.fields(field_sel);
        }

        trace!(
            label_selector = ?filters.label_selector,
            field_selector = ?filters.field_selector,
            "Built ListParams"
        );

        params
    }

    /// Get cache statistics (hits, misses)
    #[allow(dead_code)]
    pub fn cache_stats(&self) -> (usize, usize) {
        (
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
        )
    }

    /// Stream resources from Kubernetes API as pages arrive
    ///
    /// This method yields pages of resources as they are fetched from the K8s API,
    /// enabling streaming execution and early termination with LIMIT.
    /// Each yielded Vec contains one page of resources (up to PAGE_SIZE items).
    pub fn stream_resources(
        &self,
        table: &str,
        namespace: Option<&str>,
        context: Option<&str>,
        api_filters: &ApiFilters,
        limit: Option<usize>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<serde_json::Value>>> + Send + '_>> {
        let table = table.to_string();
        let namespace = namespace.map(String::from);
        let context = context.map(String::from);
        let api_filters = api_filters.clone();

        Box::pin(stream! {
            // Look up resource info from discovery
            let resource_info = match self.get_resource_info(&table, context.as_deref()).await {
                Ok(Some(info)) => info,
                Ok(None) => {
                    yield Err(anyhow!(
                        "Unknown table: '{}'. Run SHOW TABLES to see available resources.",
                        table
                    ));
                    return;
                }
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            let ctx_name = context.as_deref().unwrap_or("default");

            debug!(
                table = %table,
                cluster = %ctx_name,
                namespace = ?namespace,
                limit = ?limit,
                group = %resource_info.group,
                version = %resource_info.version,
                kind = %resource_info.api_resource.kind,
                "Streaming K8s resource"
            );

            let client = match self.get_client(context.as_deref()).await {
                Ok(c) => c,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            let ar = &resource_info.api_resource;
            let base_params = self.build_list_params(&api_filters);

            // Create API handle based on resource scope
            let api: Api<DynamicObject> = if resource_info.is_namespaced() {
                match &namespace {
                    Some(ns) => Api::namespaced_with(client, ns, ar),
                    None => Api::all_with(client, ar),
                }
            } else {
                Api::all_with(client, ar)
            };

            // Build apiVersion string (e.g., "v1", "apps/v1", "cert-manager.io/v1")
            let api_version = if resource_info.group.is_empty() {
                resource_info.version.clone()
            } else {
                format!("{}/{}", resource_info.group, resource_info.version)
            };
            let kind = resource_info.api_resource.kind.clone();

            // Stream pages with pagination
            let mut continue_token: Option<String> = None;
            let mut total_fetched = 0usize;
            let mut page_count = 0u32;

            loop {
                // Build params for this page
                let mut params = base_params.clone().limit(PAGE_SIZE);
                if let Some(ref token) = continue_token {
                    params = params.continue_token(token);
                }

                // Fetch page with retry
                let list = match self.list_page_with_retry(&api, &params, &table, ctx_name).await {
                    Ok(l) => l,
                    Err(e) => {
                        yield Err(e);
                        return;
                    }
                };

                let items_count = list.items.len();
                page_count += 1;

                // Convert items to JSON values with apiVersion and kind
                let mut values: Vec<serde_json::Value> = list
                    .items
                    .into_iter()
                    .map(|item| {
                        let mut value = serde_json::to_value(item).unwrap_or(serde_json::Value::Null);
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

                // Truncate to limit if needed (before yielding)
                let mut hit_limit = false;
                if let Some(max) = limit {
                    let remaining = max.saturating_sub(total_fetched);
                    if remaining < values.len() {
                        values.truncate(remaining);
                        hit_limit = true;
                    }
                }

                let yielded_count = values.len();
                total_fetched += yielded_count;

                debug!(
                    table = %table,
                    context = %ctx_name,
                    page = page_count,
                    items_this_page = items_count,
                    yielded = yielded_count,
                    total_so_far = total_fetched,
                    "Streaming page"
                );

                // Yield this page (possibly truncated)
                yield Ok(values);

                // Stop if we hit the limit
                if hit_limit {
                    debug!(
                        table = %table,
                        context = %ctx_name,
                        limit = ?limit,
                        total_fetched = total_fetched,
                        "Reached limit, stopping pagination"
                    );
                    break;
                }

                // Check for more pages
                match list.metadata.continue_ {
                    Some(token) if !token.is_empty() => {
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
                    total_items = total_fetched,
                    "Streaming pagination complete"
                );
            }
        })
    }
}
