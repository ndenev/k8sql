mod cache;
mod client;
pub mod discovery;

pub use cache::ResourceCache;
pub use client::{K8sClientPool, extract_initial_context, is_multi_or_pattern_spec};

/// Parameters to push down to the Kubernetes API
#[derive(Debug, Clone, Default)]
pub struct ApiFilters {
    /// Label selector string (e.g., "app=nginx,version=v1")
    pub label_selector: Option<String>,
    /// Field selector string (e.g., "status.phase=Running")
    pub field_selector: Option<String>,
}
