// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! DataFusion integration for k8sql
//!
//! This module provides a DataFusion TableProvider implementation for Kubernetes resources,
//! allowing full SQL support including JOINs, aggregations, and subqueries.

mod context;
mod convert;
mod execution;
mod filter_extraction;
mod hooks;
mod json_path;
mod preprocess;
mod provider;
pub mod prql;

pub use context::{K8sSessionContext, TableInfo};
pub use hooks::{SetConfigHook, ShowDatabasesHook, ShowTablesHook};
