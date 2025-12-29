// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Progress reporting for long-running operations
//!
//! Provides a way for the K8s provider to report progress during queries,
//! which the REPL can display to the user.

use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;

/// Create a spinner with consistent styling
pub fn create_spinner(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
            .template("{spinner:.cyan} {msg} {elapsed:.dim}")
            .unwrap(),
    );
    pb.set_message(msg.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

/// Progress update message
#[derive(Clone, Debug)]
pub enum ProgressUpdate {
    // === Connection/Discovery phases ===
    /// Connecting to a cluster
    Connecting { cluster: String },
    /// Connected to a cluster
    Connected { cluster: String, elapsed_ms: u64 },
    /// Discovering resources on a cluster
    Discovering { cluster: String },
    /// Discovery complete for a cluster
    DiscoveryComplete {
        cluster: String,
        table_count: usize,
        elapsed_ms: u64,
    },
    /// Registering tables
    RegisteringTables { count: usize },

    // === Query phases ===
    /// Starting to query clusters
    StartingQuery { table: String, cluster_count: usize },
    /// Fetched data from a cluster/partition
    ClusterComplete {
        cluster: String,
        rows: usize,
        elapsed_ms: u64,
    },
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

    /// Report cluster/partition completion
    pub fn cluster_complete(&self, cluster: &str, rows: usize, elapsed_ms: u64) {
        self.clusters_done.fetch_add(1, Ordering::SeqCst);
        let _ = self.sender.send(ProgressUpdate::ClusterComplete {
            cluster: cluster.to_string(),
            rows,
            elapsed_ms,
        });
    }

    /// Report connecting to a cluster
    pub fn connecting(&self, cluster: &str) {
        let _ = self.sender.send(ProgressUpdate::Connecting {
            cluster: cluster.to_string(),
        });
    }

    /// Report connected to a cluster
    pub fn connected(&self, cluster: &str, elapsed_ms: u64) {
        let _ = self.sender.send(ProgressUpdate::Connected {
            cluster: cluster.to_string(),
            elapsed_ms,
        });
    }

    /// Report discovering resources on a cluster
    pub fn discovering(&self, cluster: &str) {
        let _ = self.sender.send(ProgressUpdate::Discovering {
            cluster: cluster.to_string(),
        });
    }

    /// Report discovery complete for a cluster
    pub fn discovery_complete(&self, cluster: &str, table_count: usize, elapsed_ms: u64) {
        let _ = self.sender.send(ProgressUpdate::DiscoveryComplete {
            cluster: cluster.to_string(),
            table_count,
            elapsed_ms,
        });
    }

    /// Report registering tables
    pub fn registering_tables(&self, count: usize) {
        let _ = self
            .sender
            .send(ProgressUpdate::RegisteringTables { count });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_progress_reporter_new() {
        let reporter = ProgressReporter::new();
        assert_eq!(reporter.progress(), (0, 0));
    }

    #[test]
    fn test_progress_reporter_default() {
        let reporter = ProgressReporter::default();
        assert_eq!(reporter.progress(), (0, 0));
    }

    #[test]
    fn test_start_query() {
        let reporter = ProgressReporter::new();
        reporter.start_query("pods", 3);

        assert_eq!(reporter.progress(), (0, 3));
    }

    #[test]
    fn test_cluster_complete_increments() {
        let reporter = ProgressReporter::new();
        reporter.start_query("pods", 3);

        reporter.cluster_complete("cluster-1", 10, 100);
        assert_eq!(reporter.progress(), (1, 3));

        reporter.cluster_complete("cluster-2", 20, 150);
        assert_eq!(reporter.progress(), (2, 3));

        reporter.cluster_complete("cluster-3", 15, 200);
        assert_eq!(reporter.progress(), (3, 3));
    }

    #[test]
    fn test_subscribe_receives_updates() {
        let reporter = ProgressReporter::new();
        let mut receiver = reporter.subscribe();

        reporter.start_query("pods", 2);

        // Check we received the update
        let update = receiver.try_recv().unwrap();
        match update {
            ProgressUpdate::StartingQuery {
                table,
                cluster_count,
            } => {
                assert_eq!(table, "pods");
                assert_eq!(cluster_count, 2);
            }
            _ => panic!("Expected StartingQuery update"),
        }
    }

    #[test]
    fn test_multiple_updates() {
        let reporter = ProgressReporter::new();
        let mut receiver = reporter.subscribe();

        // Send various updates
        reporter.connecting("cluster-1");
        reporter.connected("cluster-1", 50);
        reporter.discovering("cluster-1");
        reporter.discovery_complete("cluster-1", 100, 200);
        reporter.registering_tables(100);
        reporter.start_query("pods", 1);
        reporter.cluster_complete("cluster-1", 25, 100);

        // Verify all updates were received
        let updates: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
        assert_eq!(updates.len(), 7);

        // Check types of updates
        assert!(matches!(updates[0], ProgressUpdate::Connecting { .. }));
        assert!(matches!(updates[1], ProgressUpdate::Connected { .. }));
        assert!(matches!(updates[2], ProgressUpdate::Discovering { .. }));
        assert!(matches!(
            updates[3],
            ProgressUpdate::DiscoveryComplete { .. }
        ));
        assert!(matches!(
            updates[4],
            ProgressUpdate::RegisteringTables { .. }
        ));
        assert!(matches!(updates[5], ProgressUpdate::StartingQuery { .. }));
        assert!(matches!(updates[6], ProgressUpdate::ClusterComplete { .. }));
    }

    #[test]
    fn test_create_progress_handle() {
        let handle = create_progress_handle();
        assert_eq!(handle.progress(), (0, 0));
    }

    #[test]
    fn test_start_query_resets_counters() {
        let reporter = ProgressReporter::new();

        // First query
        reporter.start_query("pods", 3);
        reporter.cluster_complete("c1", 10, 100);
        reporter.cluster_complete("c2", 10, 100);
        assert_eq!(reporter.progress(), (2, 3));

        // Start a new query - should reset
        reporter.start_query("deployments", 2);
        assert_eq!(reporter.progress(), (0, 2));
    }

    #[test]
    fn test_progress_update_clone_and_debug() {
        // Verify Clone and Debug traits work
        let update = ProgressUpdate::StartingQuery {
            table: "pods".to_string(),
            cluster_count: 3,
        };

        let cloned = update.clone();
        let debug_str = format!("{:?}", cloned);
        assert!(debug_str.contains("StartingQuery"));
        assert!(debug_str.contains("pods"));
    }
}
