// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! SQL-related utilities for k8sql
//!
//! Most SQL parsing and execution is now handled by DataFusion.
//! This module provides types for API filter pushdown.

/// Parameters to push down to the Kubernetes API
#[derive(Debug, Clone, Default)]
pub struct ApiFilters {
    /// Label selector string (e.g., "app=nginx,version=v1")
    pub label_selector: Option<String>,
    /// Field selector string (e.g., "status.phase=Running")
    pub field_selector: Option<String>,
}
