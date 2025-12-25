// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Configuration persistence for k8sql
//!
//! Stores user preferences like selected clusters in a config file.
//! Config location: ~/.config/k8sql/config.toml (XDG) or ~/.k8sql/config.toml

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

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
            let config: Config = toml::from_str(&content)
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

        let content = toml::to_string_pretty(self)
            .context("Failed to serialize config")?;
        fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;

        Ok(())
    }

    /// Get the config file path
    /// Uses XDG config directory (~/.config/k8sql/) or falls back to ~/.k8sql/
    pub fn config_path() -> Result<PathBuf> {
        let config_dir = dirs::config_dir()
            .map(|p| p.join("k8sql"))
            .or_else(|| dirs::home_dir().map(|p| p.join(".k8sql")))
            .context("Could not determine config directory")?;

        Ok(config_dir.join("config.toml"))
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
        let toml = toml::to_string_pretty(&config).unwrap();
        assert!(toml.contains("selected_contexts"));
        assert!(toml.contains("prod"));
        assert!(toml.contains("staging"));
    }

    #[test]
    fn test_config_deserialize() {
        let toml = r#"
selected_contexts = ["cluster1", "cluster2"]
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.selected_contexts, vec!["cluster1", "cluster2"]);
    }

    #[test]
    fn test_config_deserialize_empty() {
        let toml = "";
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.selected_contexts.is_empty());
    }

    #[test]
    fn test_config_roundtrip() {
        let original = Config {
            selected_contexts: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        let toml = toml::to_string_pretty(&original).unwrap();
        let parsed: Config = toml::from_str(&toml).unwrap();
        assert_eq!(original.selected_contexts, parsed.selected_contexts);
    }

    // Integration test with temp directory
    fn test_config_with_temp_dir<F>(test_fn: F)
    where
        F: FnOnce(PathBuf),
    {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        test_fn(config_path);
    }

    #[test]
    fn test_config_save_and_load() {
        test_config_with_temp_dir(|path| {
            // Save config
            let config = Config {
                selected_contexts: vec!["test-cluster".to_string()],
            };
            let content = toml::to_string_pretty(&config).unwrap();
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, content).unwrap();

            // Load and verify
            let loaded_content = fs::read_to_string(&path).unwrap();
            let loaded: Config = toml::from_str(&loaded_content).unwrap();
            assert_eq!(loaded.selected_contexts, vec!["test-cluster"]);
        });
    }
}
