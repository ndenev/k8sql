// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Progress reporting for long-running operations
//!
//! Provides a way for the K8s provider to report progress during queries,
//! which the REPL can display to the user.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

/// Progress update message
#[derive(Clone, Debug)]
pub enum ProgressUpdate {
    /// Starting to query clusters
    StartingQuery { table: String, cluster_count: usize },
    /// Fetched data from a cluster
    ClusterComplete {
        cluster: String,
        rows: usize,
        elapsed_ms: u64,
    },
    /// All clusters complete
    QueryComplete { total_rows: usize, elapsed_ms: u64 },
}

/// Global progress reporter
pub struct ProgressReporter {
    sender: broadcast::Sender<ProgressUpdate>,
    /// Count of completed clusters for current query
    clusters_done: AtomicUsize,
    /// Total clusters for current query
    clusters_total: AtomicUsize,
}

impl ProgressReporter {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(100);
        Self {
            sender,
            clusters_done: AtomicUsize::new(0),
            clusters_total: AtomicUsize::new(0),
        }
    }

    /// Subscribe to progress updates
    pub fn subscribe(&self) -> broadcast::Receiver<ProgressUpdate> {
        self.sender.subscribe()
    }

    /// Report query start
    pub fn start_query(&self, table: &str, cluster_count: usize) {
        self.clusters_done.store(0, Ordering::SeqCst);
        self.clusters_total.store(cluster_count, Ordering::SeqCst);
        let _ = self.sender.send(ProgressUpdate::StartingQuery {
            table: table.to_string(),
            cluster_count,
        });
    }

    /// Report cluster completion
    pub fn cluster_complete(&self, cluster: &str, rows: usize, elapsed_ms: u64) {
        self.clusters_done.fetch_add(1, Ordering::SeqCst);
        let _ = self.sender.send(ProgressUpdate::ClusterComplete {
            cluster: cluster.to_string(),
            rows,
            elapsed_ms,
        });
    }

    /// Report query completion
    pub fn query_complete(&self, total_rows: usize, elapsed_ms: u64) {
        let _ = self.sender.send(ProgressUpdate::QueryComplete {
            total_rows,
            elapsed_ms,
        });
    }

    /// Get current progress (done/total)
    pub fn progress(&self) -> (usize, usize) {
        (
            self.clusters_done.load(Ordering::SeqCst),
            self.clusters_total.load(Ordering::SeqCst),
        )
    }
}

impl Default for ProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe handle to progress reporter
pub type ProgressHandle = Arc<ProgressReporter>;

/// Create a new progress reporter handle
pub fn create_progress_handle() -> ProgressHandle {
    Arc::new(ProgressReporter::new())
}
