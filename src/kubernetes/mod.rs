mod cache;
mod client;
mod context_matcher;
pub mod discovery;
pub mod field_selectors;

pub use cache::ResourceCache;
pub use client::{K8sClientPool, extract_initial_context, is_multi_or_pattern_spec};
pub use field_selectors::{FieldSelector, FieldSelectorOperator, FieldSelectorRegistry};

/// Parameters to push down to the Kubernetes API
#[derive(Debug, Clone, Default)]
pub struct ApiFilters {
    /// Label selector string (e.g., "app=nginx,version=v1")
    pub label_selector: Option<String>,
    /// Field selector string (e.g., "status.phase=Running")
    pub field_selector: Option<String>,
}
