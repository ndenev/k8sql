// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Configuration persistence for k8sql
//!
//! Stores user preferences like selected clusters in a config file.
//! All k8sql data is stored under ~/.k8sql/:
//! - ~/.k8sql/config.json - user configuration
//! - ~/.k8sql/cache/ - resource discovery cache
//! - ~/.k8sql/history - REPL command history

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Get the base k8sql directory (~/.k8sql/)
pub fn base_dir() -> Result<PathBuf> {
    dirs::home_dir()
        .map(|p| p.join(".k8sql"))
        .context("Could not determine home directory")
}

/// k8sql configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Selected cluster contexts (used by default for queries)
    #[serde(default)]
    pub selected_contexts: Vec<String>,
}

impl Config {
    /// Load config from disk, or return default if not found
    pub fn load() -> Result<Self> {
        let path = Self::config_path()?;
        if path.exists() {
            let content = fs::read_to_string(&path)
                .with_context(|| format!("Failed to read config file: {}", path.display()))?;
            let config: Config = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    /// Save config to disk
    pub fn save(&self) -> Result<()> {
        let path = Self::config_path()?;

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
        }

        let content = serde_json::to_string_pretty(self)
            .context("Failed to serialize config")?;
        fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;

        Ok(())
    }

    /// Get the config file path (~/.k8sql/config.json)
    pub fn config_path() -> Result<PathBuf> {
        Ok(base_dir()?.join("config.json"))
    }

    /// Update selected contexts and save
    pub fn set_selected_contexts(&mut self, contexts: Vec<String>) -> Result<()> {
        self.selected_contexts = contexts;
        self.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert!(config.selected_contexts.is_empty());
    }

    #[test]
    fn test_config_serialize() {
        let config = Config {
            selected_contexts: vec!["prod".to_string(), "staging".to_string()],
        };
        let json = serde_json::to_string_pretty(&config).unwrap();
        assert!(json.contains("selected_contexts"));
        assert!(json.contains("prod"));
        assert!(json.contains("staging"));
    }

    #[test]
    fn test_config_deserialize() {
        let json = r#"{"selected_contexts": ["cluster1", "cluster2"]}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.selected_contexts, vec!["cluster1", "cluster2"]);
    }

    #[test]
    fn test_config_deserialize_empty() {
        let json = "{}";
        let config: Config = serde_json::from_str(json).unwrap();
        assert!(config.selected_contexts.is_empty());
    }

    #[test]
    fn test_config_roundtrip() {
        let original = Config {
            selected_contexts: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let json = serde_json::to_string_pretty(&original).unwrap();
        let parsed: Config = serde_json::from_str(&json).unwrap();
        assert_eq!(original.selected_contexts, parsed.selected_contexts);
    }

    #[test]
    fn test_config_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.json");

        // Save config
        let config = Config {
            selected_contexts: vec!["test-cluster".to_string()],
        };
        let content = serde_json::to_string_pretty(&config).unwrap();
        fs::write(&config_path, content).unwrap();

        // Load and verify
        let loaded_content = fs::read_to_string(&config_path).unwrap();
        let loaded: Config = serde_json::from_str(&loaded_content).unwrap();
        assert_eq!(loaded.selected_contexts, vec!["test-cluster"]);
    }
}
