use anyhow::{Context, Result, anyhow};
use async_stream::stream;
use futures::Stream;
use kube::api::DynamicObject;
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client, Config, api::ListParams};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};

use super::ApiFilters;
use super::cache::{CachedResourceInfo, ResourceCache};
use super::discovery::{ResourceInfo, ResourceRegistry};
use crate::progress::ProgressHandle;

/// How long to cache discovered resources before auto-refresh
const REGISTRY_TTL: Duration = Duration::from_secs(1800); // 30 minutes

/// Timeout for connecting to K8s API
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for reading K8s API responses
const READ_TIMEOUT: Duration = Duration::from_secs(30);

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
}

/// Extract (group, version, kind) tuples from CRD list
/// Only includes served versions (v.served == true)
fn extract_served_crd_versions(
    crd_list: &kube::api::ObjectList<
        k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    >,
) -> Vec<(String, String, String)> {
    crd_list
        .items
        .iter()
        .flat_map(|crd| {
            crd.spec.versions.iter().filter(|v| v.served).map(move |v| {
                (
                    crd.spec.group.clone(),
                    v.name.clone(),
                    crd.spec.names.kind.clone(),
                )
            })
        })
        .collect()
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

    /// Load CRDs using per-CRD caching
    /// Returns the number of CRDs loaded
    ///
    /// Strategy:
    /// 1. Try to load saved cluster CRD list from cache (no API call)
    /// 2. If cache is fresh and complete, use it directly
    /// 3. Otherwise, fetch full CRD list from cluster
    /// 4. Load cached CRDs, discover only missing CRDs
    async fn load_crds_with_cache(
        &self,
        context: &str,
        client: &Client,
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
        use kube::{Api, api::ObjectList};

        // Step 1: Try to load from cache first (no API call)
        if let Some(cached_crds) = self.resource_cache.load_cluster_groups(context) {
            let (cached_resources, missing_crds) = self.resource_cache.check_crds(&cached_crds);

            // If all CRDs are cached, load directly without any API calls
            if missing_crds.is_empty() {
                let count = cached_resources.len();
                for cached_resource in &cached_resources {
                    registry.add(cached_resource.clone().into());
                }
                info!(
                    context = %context,
                    cached = count,
                    "Loaded all CRDs from cache (no API calls)"
                );
                return Ok(count);
            }
            // Some CRDs missing - fall through to API call
        }

        // Step 2: Fetch full CRD list from cluster (single API call)
        let api: Api<CustomResourceDefinition> = Api::all(client.clone());
        let crd_list: ObjectList<CustomResourceDefinition> = api.list(&Default::default()).await?;

        if crd_list.items.is_empty() {
            return Ok(0);
        }

        // Step 3: Extract (group, version, kind) tuples from CRD list
        let all_crds = extract_served_crd_versions(&crd_list);

        // Step 4: Check which CRDs are cached vs missing
        let (cached_resources, missing_crds) = self.resource_cache.check_crds(&all_crds);

        let cached_count = cached_resources.len();
        let missing_count = missing_crds.len();

        // Step 5: Load cached CRDs into registry
        for cached_resource in &cached_resources {
            registry.add(cached_resource.clone().into());
        }

        // Step 6: Discover missing CRDs (if any)
        let discovered_count = if !missing_crds.is_empty() {
            info!(
                context = %context,
                cached = cached_count,
                missing = missing_count,
                "Loading CRDs (cached: {}, discovering: {})",
                cached_count,
                missing_count
            );

            self.process_discovered_crds(client, context, &missing_crds, &crd_list, registry)
                .await?
        } else {
            info!(
                context = %context,
                cached = cached_count,
                "Loaded all CRDs from cache"
            );
            0
        };

        // Step 7: Save cluster CRD list for next time
        if let Err(e) = self.resource_cache.save_cluster_groups(context, &all_crds) {
            warn!(context = %context, error = %e, "Failed to save cluster CRDs");
        }

        // Total CRD count
        let crd_count = cached_count + discovered_count;

        Ok(crd_count)
    }

    /// Force rediscover all CRDs from the cluster
    /// Gets fresh list of CRDs but uses cached CRD data when available
    async fn discover_all_crds(
        &self,
        context: &str,
        client: &Client,
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
        use kube::{Api, api::ObjectList};

        // Fetch full CRD list from cluster (single API call)
        let api: Api<CustomResourceDefinition> = Api::all(client.clone());
        let crd_list: ObjectList<CustomResourceDefinition> = api.list(&Default::default()).await?;

        if crd_list.items.is_empty() {
            return Ok(0);
        }

        // Extract (group, version, kind) tuples from CRD list
        let all_crds = extract_served_crd_versions(&crd_list);

        // Check which CRDs are cached vs missing (shared cache across clusters)
        let (cached_resources, missing_crds) = self.resource_cache.check_crds(&all_crds);

        let cached_count = cached_resources.len();
        let missing_count = missing_crds.len();

        // Load cached CRDs into registry
        let mut crd_count = 0;
        for cached_resource in &cached_resources {
            registry.add(cached_resource.clone().into());
            crd_count += 1;
        }

        // Discover only missing CRDs (if any)
        if !missing_crds.is_empty() {
            info!(
                context = %context,
                cached = cached_count,
                missing = missing_count,
                "Force refresh: loading CRDs (cached: {}, discovering: {})",
                cached_count,
                missing_count
            );

            crd_count += self
                .process_discovered_crds(client, context, &missing_crds, &crd_list, registry)
                .await?;
        } else {
            info!(
                context = %context,
                cached = cached_count,
                "Force refresh: all CRDs loaded from shared cache"
            );
        }

        // Save cluster CRD list
        if let Err(e) = self.resource_cache.save_cluster_groups(context, &all_crds) {
            warn!(context = %context, error = %e, "Failed to save cluster CRDs");
        }

        Ok(crd_count)
    }

    /// Process newly discovered CRDs: build ResourceInfo, cache, and add to registry
    ///
    /// This helper consolidates the common pattern of:
    /// 1. Finding CRD schemas from the pre-fetched CRD list
    /// 2. Building ResourceInfo for each CRD
    /// 3. Saving individual CRDs to the local cache
    /// 4. Adding to the in-memory registry
    async fn process_discovered_crds(
        &self,
        _client: &Client,
        context: &str,
        missing_crds: &[(String, String, String)],
        crd_list: &kube::api::ObjectList<k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition>,
        registry: &mut ResourceRegistry,
    ) -> Result<usize> {
        use kube::discovery::ApiCapabilities;

        let mut count = 0;

        for (group, version, kind) in missing_crds {
            // Find CRD schema from the already-fetched list
            let Some(schema_result) = super::discovery::find_crd_schema(group, kind, crd_list)
            else {
                tracing::debug!(
                    context = %context,
                    kind = %kind,
                    group = %group,
                    "CRD not found in cluster, skipping"
                );
                continue;
            };

            // Build API version string
            let api_version = if group.is_empty() {
                version.clone()
            } else {
                format!("{}/{}", group, version)
            };

            // Destructure schema_result to avoid cloning (move all fields at once)
            let super::discovery::SchemaResult {
                fields,
                short_names,
                plural,
                singular,
                scope,
            } = schema_result;

            // Build aliases: start with shortNames, add singular if available
            let mut aliases = short_names; // Move instead of clone

            // Always add singular as additional alias if available (user preference)
            if let Some(singular) = singular {
                let singular_lower = singular.to_lowercase();
                // Only add if different from plural and not already in shortNames
                if singular_lower != plural.to_lowercase() && !aliases.contains(&singular_lower) {
                    aliases.push(singular_lower);
                }
            }

            // Create ResourceInfo
            let resource_info = super::discovery::ResourceInfo {
                api_resource: kube::discovery::ApiResource {
                    group: group.clone(),
                    version: version.clone(),
                    api_version,
                    kind: kind.clone(),
                    plural: plural.clone(),
                },
                capabilities: ApiCapabilities {
                    scope,
                    subresources: vec![],
                    operations: vec![],
                },
                table_name: plural.to_lowercase(),
                aliases,
                is_core: false,
                group: group.clone(),
                version: version.clone(),
                custom_fields: fields,
            };

            // Convert to cached format
            let cached_resource = CachedResourceInfo::from(&resource_info);

            // Save individual CRD to cache
            if let Err(e) = self
                .resource_cache
                .save_crd(group, version, kind, &cached_resource)
            {
                warn!(
                    context = %context,
                    kind = %kind,
                    group = %group,
                    error = %e,
                    "Failed to cache CRD"
                );
            }

            // Add to registry
            registry.add(resource_info);
            count += 1;
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
                // Emit connected event even for cached client
                self.progress
                    .connected(context, start.elapsed().as_millis() as u64);
                return Ok(client.clone());
            }
        }

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
        // Retry up to 3 times for intermittent failures
        let discovery_futures: Vec<_> = matched_contexts
            .iter()
            .map(|ctx| {
                let ctx = ctx.clone();
                async move {
                    let mut last_error = None;
                    for attempt in 1..=3 {
                        match self.get_or_create_client(&ctx).await {
                            Ok(_) => {
                                // Client connected, now discover resources
                                match self
                                    .discover_resources_for_context(&ctx, force_refresh)
                                    .await
                                {
                                    Ok(_) => return Ok::<_, anyhow::Error>(()),
                                    Err(e) => {
                                        warn!(
                                            context = %ctx,
                                            attempt = attempt,
                                            error = %e,
                                            "Discovery failed, retrying..."
                                        );
                                        last_error = Some(e);
                                        if attempt < 3 {
                                            tokio::time::sleep(std::time::Duration::from_millis(
                                                100 * attempt as u64,
                                            ))
                                            .await;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    context = %ctx,
                                    attempt = attempt,
                                    error = %e,
                                    "Connection failed, retrying..."
                                );
                                last_error = Some(e);
                                if attempt < 3 {
                                    tokio::time::sleep(std::time::Duration::from_millis(
                                        100 * attempt as u64,
                                    ))
                                    .await;
                                }
                            }
                        }
                    }
                    Err(last_error
                        .expect("retry loop should always set last_error after 3 failed attempts")
                        .context(format!("Failed after 3 attempts for cluster '{}'", ctx)))
                }
            })
            .collect();

        let results = futures::future::join_all(discovery_futures).await;

        // Check for any errors - fail if ANY context fails (after retries)
        // This ensures predictable behavior for scripting and avoids misleading partial success
        let mut failed_contexts = Vec::new();
        for (ctx, result) in matched_contexts.iter().zip(results.iter()) {
            if let Err(e) = result {
                failed_contexts.push((ctx.clone(), e.to_string()));
            }
        }

        if !failed_contexts.is_empty() {
            let error_summary = failed_contexts
                .iter()
                .map(|(ctx, e)| format!("  - {}: {}", ctx, e))
                .collect::<Vec<_>>()
                .join("\n");
            return Err(anyhow!(
                "Failed to load {} of {} requested contexts:\n{}",
                failed_contexts.len(),
                matched_contexts.len(),
                error_summary
            ));
        }

        // All contexts loaded successfully - update current contexts
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
        const MAX_RETRIES: u32 = 3;
        const RETRY_BASE_DELAY_MS: u64 = 100;

        // Check if a kube error is retryable (transient failures)
        let is_retryable = |err: &kube::Error| -> bool {
            match err {
                kube::Error::HyperError(_) => true,
                kube::Error::Api(api_err) => {
                    matches!(api_err.code, 408 | 429 | 500 | 502 | 503 | 504)
                }
                _ => false,
            }
        };

        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            match api.list(params).await {
                Ok(list) => return Ok(list),
                Err(e) => {
                    if is_retryable(&e) {
                        let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * 2u64.pow(attempt));
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
                    // Table not in discovery cache for this cluster - return empty results
                    // This is expected in wildcard queries where different clusters have different CRDs
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
                // If we have a limit, only request what we need (up to PAGE_SIZE)
                let page_limit = if let Some(max_limit) = limit {
                    let remaining = max_limit.saturating_sub(total_fetched);
                    if remaining == 0 {
                        break; // We've fetched enough, stop pagination
                    }
                    PAGE_SIZE.min(remaining as u32)
                } else {
                    PAGE_SIZE
                };

                let mut params = base_params.clone().limit(page_limit);
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
                // Only insert if not already present (K8s API usually includes them)
                let mut values: Vec<serde_json::Value> = list
                    .items
                    .into_iter()
                    .map(|item| {
                        let mut value = serde_json::to_value(item).unwrap_or(serde_json::Value::Null);
                        if let serde_json::Value::Object(ref mut map) = value {
                            // Only insert if missing (avoid redundant work)
                            if !map.contains_key("apiVersion") {
                                map.insert(
                                    "apiVersion".to_string(),
                                    serde_json::Value::String(api_version.clone()),
                                );
                            }
                            if !map.contains_key("kind") {
                                map.insert("kind".to_string(), serde_json::Value::String(kind.clone()));
                            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function to test alias building logic in isolation
    /// This replicates the logic from process_discovered_crds
    fn build_aliases(
        short_names: Vec<String>,
        plural: String,
        singular: Option<String>,
    ) -> Vec<String> {
        let mut aliases = short_names;

        if let Some(singular) = singular {
            let singular_lower = singular.to_lowercase();
            if singular_lower != plural.to_lowercase() && !aliases.contains(&singular_lower) {
                aliases.push(singular_lower);
            }
        }

        aliases
    }

    #[test]
    fn test_singular_alias_different_from_plural() {
        // CRD with plural "certificates" and singular "certificate"
        let short_names = vec!["cert".to_string(), "certs".to_string()];
        let plural = "certificates".to_string();
        let singular = Some("certificate".to_string());

        let aliases = build_aliases(short_names, plural, singular);

        assert_eq!(aliases.len(), 3);
        assert!(aliases.contains(&"cert".to_string()));
        assert!(aliases.contains(&"certs".to_string()));
        assert!(aliases.contains(&"certificate".to_string()));
    }

    #[test]
    fn test_singular_same_as_plural() {
        // Edge case: singular and plural are the same
        let short_names = vec!["data".to_string()];
        let plural = "data".to_string();
        let singular = Some("data".to_string());

        let aliases = build_aliases(short_names, plural, singular);

        // Should not add duplicate
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "data");
    }

    #[test]
    fn test_singular_already_in_short_names() {
        // CRD where singular is already in shortNames
        let short_names = vec!["pod".to_string(), "po".to_string()];
        let plural = "pods".to_string();
        let singular = Some("pod".to_string());

        let aliases = build_aliases(short_names, plural, singular);

        // Should not add duplicate
        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains(&"pod".to_string()));
        assert!(aliases.contains(&"po".to_string()));
    }

    #[test]
    fn test_no_singular_name() {
        // CRD without singular name defined
        let short_names = vec!["pl".to_string()];
        let plural = "podlogs".to_string();
        let singular = None;

        let aliases = build_aliases(short_names, plural, singular);

        // Should only have shortNames
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "pl");
    }

    #[test]
    fn test_empty_short_names_with_singular() {
        // CRD with no shortNames but has singular
        let short_names = vec![];
        let plural = "podlogs".to_string();
        let singular = Some("podlog".to_string());

        let aliases = build_aliases(short_names, plural, singular);

        // Should add singular as the only alias
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0], "podlog");
    }

    #[test]
    fn test_case_insensitive_comparison() {
        // Test that comparison is case-insensitive
        let short_names = vec!["cert".to_string()];
        let plural = "Certificates".to_string();
        let singular = Some("Certificate".to_string());

        let aliases = build_aliases(short_names, plural, singular);

        // Should add "certificate" (lowercased) as alias
        assert_eq!(aliases.len(), 2);
        assert!(aliases.contains(&"cert".to_string()));
        assert!(aliases.contains(&"certificate".to_string()));
    }
}
