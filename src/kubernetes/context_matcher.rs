// Copyright (c) 2025 Nikolay Denev <ndenev@gmail.com>
// SPDX-License-Identifier: BSD-3-Clause

//! Context pattern matching and resolution
//!
//! Handles parsing and matching context specifications (exact names, globs, comma-separated lists)
//! against available Kubernetes contexts from kubeconfig.

use anyhow::{Result, anyhow};

/// Resolves context specifications to concrete context names
///
/// Supports:
/// - Exact names: "prod"
/// - Glob patterns: "prod-*", "staging-?"
/// - Comma-separated lists: "prod, staging"
/// - Mixed: "prod-*, staging-01"
pub struct ContextMatcher<'a> {
    available_contexts: &'a [String],
}

impl<'a> ContextMatcher<'a> {
    /// Create a new matcher with the available contexts
    pub fn new(available_contexts: &'a [String]) -> Self {
        Self { available_contexts }
    }

    /// Resolve a context specification to a list of matching context names
    ///
    /// Returns an error if:
    /// - An exact match is requested but the context doesn't exist
    /// - No contexts match the pattern
    pub fn resolve(&self, spec: &str) -> Result<Vec<String>> {
        let mut matched_contexts = Vec::new();

        for part in spec.split(',') {
            let pattern = part.trim();
            if pattern.is_empty() {
                continue;
            }

            // Check if it's a glob pattern
            if pattern.contains('*') || pattern.contains('?') {
                // Glob matching - collect all matches
                for ctx in self.available_contexts {
                    if Self::glob_match(pattern, ctx) && !matched_contexts.contains(ctx) {
                        matched_contexts.push(ctx.clone());
                    }
                }
            } else {
                // Exact match
                let pattern_str = pattern.to_string();
                if self.available_contexts.contains(&pattern_str) {
                    if !matched_contexts.contains(&pattern_str) {
                        matched_contexts.push(pattern_str);
                    }
                } else {
                    return Err(anyhow!("Context '{}' not found", pattern));
                }
            }
        }

        if matched_contexts.is_empty() {
            return Err(anyhow!("No contexts matched pattern '{}'", spec));
        }

        Ok(matched_contexts)
    }

    /// Simple glob pattern matching (supports * and ?)
    /// Uses an efficient iterative algorithm without allocations
    fn glob_match(pattern: &str, text: &str) -> bool {
        let pattern: Vec<char> = pattern.chars().collect();
        let text: Vec<char> = text.chars().collect();

        let mut pi = 0; // pattern index
        let mut ti = 0; // text index
        let mut star_pi = None; // position of last '*' in pattern
        let mut star_ti = 0; // position in text when we saw last '*'

        while ti < text.len() {
            if pi < pattern.len() && (pattern[pi] == '?' || pattern[pi] == text[ti]) {
                // Character match or '?' wildcard
                pi += 1;
                ti += 1;
            } else if pi < pattern.len() && pattern[pi] == '*' {
                // '*' wildcard - remember position and try matching zero chars
                star_pi = Some(pi);
                star_ti = ti;
                pi += 1;
            } else if let Some(sp) = star_pi {
                // Mismatch, but we have a previous '*' - backtrack
                // Try matching one more character with the '*'
                pi = sp + 1;
                star_ti += 1;
                ti = star_ti;
            } else {
                // Mismatch and no '*' to backtrack to
                return false;
            }
        }

        // Check remaining pattern characters (must all be '*')
        while pi < pattern.len() && pattern[pi] == '*' {
            pi += 1;
        }

        pi == pattern.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_contexts() -> Vec<String> {
        vec![
            "prod-01".to_string(),
            "prod-02".to_string(),
            "staging-01".to_string(),
            "staging-02".to_string(),
            "dev".to_string(),
        ]
    }

    #[test]
    fn test_exact_match() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let result = matcher.resolve("prod-01").unwrap();
        assert_eq!(result, vec!["prod-01"]);
    }

    #[test]
    fn test_exact_match_not_found() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let result = matcher.resolve("nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_glob_single_star() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let mut result = matcher.resolve("prod-*").unwrap();
        result.sort();
        assert_eq!(result, vec!["prod-01", "prod-02"]);
    }

    #[test]
    fn test_glob_question_mark() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let result = matcher.resolve("de?").unwrap();
        assert_eq!(result, vec!["dev"]);
    }

    #[test]
    fn test_glob_no_matches() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let result = matcher.resolve("test-*");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("No contexts matched")
        );
    }

    #[test]
    fn test_comma_separated() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let mut result = matcher.resolve("prod-01, staging-01").unwrap();
        result.sort();
        assert_eq!(result, vec!["prod-01", "staging-01"]);
    }

    #[test]
    fn test_comma_separated_with_spaces() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let mut result = matcher.resolve("  prod-01  ,  staging-01  ").unwrap();
        result.sort();
        assert_eq!(result, vec!["prod-01", "staging-01"]);
    }

    #[test]
    fn test_mixed_exact_and_glob() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        let mut result = matcher.resolve("dev, staging-*").unwrap();
        result.sort();
        assert_eq!(result, vec!["dev", "staging-01", "staging-02"]);
    }

    #[test]
    fn test_deduplication() {
        let contexts = test_contexts();
        let matcher = ContextMatcher::new(&contexts);

        // "prod-*" matches both prod-01 and prod-02, but prod-01 is also exact
        let result = matcher.resolve("prod-01, prod-*").unwrap();
        // Should only appear once
        assert_eq!(result.iter().filter(|&c| c == "prod-01").count(), 1);
    }

    #[test]
    fn test_glob_match_simple() {
        assert!(ContextMatcher::glob_match("prod-*", "prod-01"));
        assert!(ContextMatcher::glob_match("prod-*", "prod-02"));
        assert!(!ContextMatcher::glob_match("prod-*", "staging-01"));
    }

    #[test]
    fn test_glob_match_question() {
        assert!(ContextMatcher::glob_match("de?", "dev"));
        assert!(!ContextMatcher::glob_match("de?", "develop"));
        assert!(!ContextMatcher::glob_match("de?", "de"));
    }

    #[test]
    fn test_glob_match_multiple_wildcards() {
        assert!(ContextMatcher::glob_match("*-*", "prod-01"));
        assert!(ContextMatcher::glob_match("*-*", "staging-02"));
        assert!(!ContextMatcher::glob_match("*-*", "dev"));
    }

    #[test]
    fn test_glob_match_trailing_star() {
        assert!(ContextMatcher::glob_match("prod*", "prod"));
        assert!(ContextMatcher::glob_match("prod*", "prod-01"));
        assert!(ContextMatcher::glob_match("prod*", "production"));
    }
}
