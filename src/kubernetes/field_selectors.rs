// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Kubernetes field selector support registry and types
//!
//! Field selectors allow filtering Kubernetes resources by specific field values
//! when querying the API. This module maintains a hardcoded registry of which
//! resources support which field selectors, as Kubernetes does not expose this
//! information programmatically via the API or k8s-openapi types.
//!
//! ## Field Selector Basics
//!
//! - Only `=` and `!=` operators are supported (no `in`, `notin`, `exists`)
//! - All resources support `metadata.name` and `metadata.namespace`
//! - Additional fields vary by resource type (e.g., `status.phase` for pods)
//! - Field selectors use dot notation: `status.phase=Running`
//!
//! ## Registry Maintenance
//!
//! The registry is based on Kubernetes documentation:
//! https://kubernetes.io/docs/concepts/overview/working-with-objects/field-selectors/#supported-fields
//!
//! When updating for new Kubernetes versions:
//! 1. Check the official API reference for each resource type
//! 2. Look for "Field Selectors" section in documentation
//! 3. Add new fields to the appropriate resource in `FieldSelectorRegistry::default()`
//! 4. Add corresponding test cases

use std::collections::HashMap;
use std::sync::LazyLock;

/// Represents a field selector operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSelectorOperator {
    /// Equals operator (=)
    Equals,
    /// Not equals operator (!=)
    NotEquals,
}

/// Represents an extracted field selector from a SQL expression
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSelector {
    /// Field path in Kubernetes notation (e.g., "status.phase", "metadata.name")
    pub path: String,
    /// Operator (= or !=)
    pub operator: FieldSelectorOperator,
    /// Value to match
    pub value: String,
}

impl FieldSelector {
    /// Convert to Kubernetes field selector string format
    ///
    /// Examples:
    /// - `FieldSelector { path: "status.phase", operator: Equals, value: "Running" }`
    ///   → `"status.phase=Running"`
    /// - `FieldSelector { path: "type", operator: NotEquals, value: "Opaque" }`
    ///   → `"type!=Opaque"`
    pub fn to_k8s_string(&self) -> String {
        match self.operator {
            FieldSelectorOperator::Equals => format!("{}={}", self.path, self.value),
            FieldSelectorOperator::NotEquals => format!("{}!={}", self.path, self.value),
        }
    }
}

/// Global field selector registry instance
///
/// This is initialized once on first access using LazyLock to avoid creating
/// a new HashMap on every filter extraction call (hot path optimization).
pub static FIELD_SELECTOR_REGISTRY: LazyLock<FieldSelectorRegistry> =
    LazyLock::new(FieldSelectorRegistry::new);

/// Registry of supported field selectors per Kubernetes resource type
///
/// This registry maintains a hardcoded mapping of which resources support which
/// field selectors. The information comes from Kubernetes documentation and source
/// code (specifically `GetAttrs` functions in `strategy.go` files).
pub struct FieldSelectorRegistry {
    /// Map of table_name -> supported field paths
    /// e.g., "pods" -> ["metadata.name", "spec.nodeName", "status.phase", ...]
    registry: HashMap<&'static str, Vec<&'static str>>,
}

impl FieldSelectorRegistry {
    /// Get the list of supported field selectors for a resource type
    ///
    /// Returns an empty slice if the resource type is not in the registry.
    ///
    /// # Arguments
    /// * `table_name` - The k8sql table name (e.g., "pods", "secrets")
    ///
    /// # Example
    /// ```ignore
    /// let registry = FieldSelectorRegistry::default();
    /// let pod_fields = registry.get_supported_fields("pods");
    /// assert!(pod_fields.contains(&"status.phase"));
    /// ```
    pub fn get_supported_fields(&self, table_name: &str) -> &[&'static str] {
        self.registry
            .get(table_name)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Check if a specific field selector is supported for a resource type
    ///
    /// # Arguments
    /// * `table_name` - The k8sql table name (e.g., "pods", "secrets")
    /// * `field_path` - The field path in Kubernetes notation (e.g., "status.phase")
    ///
    /// # Example
    /// ```ignore
    /// let registry = FieldSelectorRegistry::default();
    /// assert!(registry.is_supported("pods", "status.phase"));
    /// assert!(!registry.is_supported("pods", "status.unsupportedField"));
    /// ```
    pub fn is_supported(&self, table_name: &str, field_path: &str) -> bool {
        self.get_supported_fields(table_name).contains(&field_path)
    }

    /// Create a new registry with all known field selectors
    ///
    /// This includes:
    /// - Universal fields: `metadata.name` (all resources)
    ///   Note: `metadata.namespace` is intentionally excluded because k8sql uses
    ///   namespaced API endpoints for better performance instead of field selectors
    /// - Resource-specific fields as documented in Kubernetes API reference
    ///
    /// Note: This is called once by the static FIELD_SELECTOR_REGISTRY.
    /// Don't call this directly; use the static instance instead.
    fn new() -> Self {
        let mut registry = HashMap::new();

        // Universal field supported by all resources
        // Note: metadata.namespace is intentionally omitted - we use namespaced API calls instead
        let universal = vec!["metadata.name"];

        // Pods - https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/
        registry.insert("pods", {
            let mut fields = universal.clone();
            fields.extend([
                "spec.nodeName",
                "spec.restartPolicy",
                "spec.schedulerName",
                "spec.serviceAccountName",
                "spec.hostNetwork",
                "status.phase",
                "status.podIP",
                "status.nominatedNodeName",
            ]);
            fields
        });

        // Events - https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/event-v1/
        registry.insert("events", {
            let mut fields = universal.clone();
            fields.extend([
                "involvedObject.kind",
                "involvedObject.namespace",
                "involvedObject.name",
                "involvedObject.uid",
                "involvedObject.apiVersion",
                "involvedObject.resourceVersion",
                "involvedObject.fieldPath",
                "reason",
                "reportingComponent",
                "source",
                "type",
            ]);
            fields
        });

        // Secrets - https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/secret-v1/
        registry.insert("secrets", {
            let mut fields = universal.clone();
            fields.push("type");
            fields
        });

        // Namespaces - https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/namespace-v1/
        registry.insert("namespaces", {
            let mut fields = universal.clone();
            fields.push("status.phase");
            fields
        });

        // ReplicaSets - https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/replica-set-v1/
        registry.insert("replicasets", {
            let mut fields = universal.clone();
            fields.push("status.replicas");
            fields
        });

        // ReplicationControllers - https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/replication-controller-v1/
        registry.insert("replicationcontrollers", {
            let mut fields = universal.clone();
            fields.push("status.replicas");
            fields
        });

        // Jobs - https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/job-v1/
        registry.insert("jobs", {
            let mut fields = universal.clone();
            fields.push("status.successful");
            fields
        });

        // Nodes - https://kubernetes.io/docs/reference/kubernetes-api/cluster-resources/node-v1/
        registry.insert("nodes", {
            let mut fields = universal.clone();
            fields.push("spec.unschedulable");
            fields
        });

        // CertificateSigningRequests
        registry.insert("certificatesigningrequests", {
            let mut fields = universal.clone();
            fields.push("spec.signerName");
            fields
        });

        Self { registry }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_selector_to_k8s_string() {
        let selector = FieldSelector {
            path: "status.phase".to_string(),
            operator: FieldSelectorOperator::Equals,
            value: "Running".to_string(),
        };
        assert_eq!(selector.to_k8s_string(), "status.phase=Running");

        let selector_ne = FieldSelector {
            path: "type".to_string(),
            operator: FieldSelectorOperator::NotEquals,
            value: "Opaque".to_string(),
        };
        assert_eq!(selector_ne.to_k8s_string(), "type!=Opaque");
    }

    #[test]
    fn test_registry_universal_fields() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // metadata.name should be supported for all resources
        assert!(registry.is_supported("pods", "metadata.name"));
        assert!(registry.is_supported("secrets", "metadata.name"));
        assert!(registry.is_supported("events", "metadata.name"));
        assert!(registry.is_supported("namespaces", "metadata.name"));
    }

    #[test]
    fn test_registry_pod_fields() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // Pod-specific fields
        assert!(registry.is_supported("pods", "status.phase"));
        assert!(registry.is_supported("pods", "spec.nodeName"));
        assert!(registry.is_supported("pods", "spec.restartPolicy"));
        assert!(registry.is_supported("pods", "status.podIP"));

        // Unsupported field
        assert!(!registry.is_supported("pods", "status.unsupportedField"));
    }

    #[test]
    fn test_registry_secret_fields() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // Secret-specific field
        assert!(registry.is_supported("secrets", "type"));

        // Unsupported field
        assert!(!registry.is_supported("secrets", "data"));
    }

    #[test]
    fn test_registry_event_fields() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // Event-specific fields
        assert!(registry.is_supported("events", "reason"));
        assert!(registry.is_supported("events", "type"));
        assert!(registry.is_supported("events", "involvedObject.kind"));
        assert!(registry.is_supported("events", "involvedObject.name"));
    }

    #[test]
    fn test_registry_namespace_not_supported() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // metadata.namespace is intentionally not supported (we use namespaced API instead)
        assert!(!registry.is_supported("pods", "metadata.namespace"));
        assert!(!registry.is_supported("secrets", "metadata.namespace"));
    }

    #[test]
    fn test_registry_unknown_resource() {
        let registry = &*FIELD_SELECTOR_REGISTRY;

        // Unknown resource type should return empty fields
        let fields = registry.get_supported_fields("unknownresource");
        assert_eq!(fields.len(), 0);

        assert!(!registry.is_supported("unknownresource", "any.field"));
    }
}
