use datafusion::logical_expr::{Expr, Operator};
use std::collections::HashSet;

/// Generic trait for filter types that can be extracted from DataFusion expressions
pub trait FilterValue: Sized {
    /// The default value when no filter is found
    fn default() -> Self;

    /// Construct from a single value
    fn from_single(value: String) -> Self;

    /// Construct from multiple values
    fn from_multiple(values: Vec<String>) -> Self;

    /// Handle special values (e.g., "*" for clusters)
    /// Returns Some(filter) if the value should be treated specially, None otherwise
    fn handle_special(value: &str) -> Option<Self> {
        let _ = value;
        None
    }

    /// Construct from negated list (e.g., NOT IN)
    /// Returns Some(filter) if negation is supported, None otherwise
    fn from_negated(values: Vec<String>) -> Option<Self> {
        let _ = values;
        None
    }
}

/// Generic filter extractor for column-based filters
pub struct FilterExtractor<'a, F: FilterValue> {
    column_name: &'a str,
    _phantom: std::marker::PhantomData<F>,
}

impl<'a, F: FilterValue> FilterExtractor<'a, F> {
    /// Create a new filter extractor for the given column
    pub fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Extract filter from DataFusion expressions
    /// Supports: column = 'x', column IN (...), and OR chains
    pub fn extract(&self, filters: &[Expr]) -> F {
        for filter in filters {
            if let Some(result) = self.extract_from_expr(filter) {
                return result;
            }
        }
        F::default()
    }

    /// Recursively extract filter from a single expression
    /// Handles AND expressions by checking both sides
    fn extract_from_expr(&self, expr: &Expr) -> Option<F> {
        match expr {
            // Handle AND expressions - check both sides
            Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                if let Some(result) = self.extract_from_expr(&binary.left) {
                    return Some(result);
                }
                self.extract_from_expr(&binary.right)
            }
            // Handle OR expressions - DataFusion transforms IN lists to OR chains
            // e.g., column IN ('a', 'b') becomes (column = 'a') OR (column = 'b')
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                let mut values = Vec::new();
                if self.collect_values_from_or(expr, &mut values) && !values.is_empty() {
                    let values = deduplicate(values);

                    // Check for special values first
                    for value in &values {
                        if let Some(special) = F::handle_special(value) {
                            return Some(special);
                        }
                    }

                    if values.len() == 1 {
                        return Some(F::from_single(values.into_iter().next().unwrap()));
                    }
                    return Some(F::from_multiple(values));
                }
                None
            }
            // Handle column = 'value'
            Expr::BinaryExpr(binary)
                if matches!(binary.op, Operator::Eq)
                    && matches!(binary.left.as_ref(), Expr::Column(col) if col.name == self.column_name) =>
            {
                if let Expr::Literal(lit, _) = binary.right.as_ref()
                    && let datafusion::common::ScalarValue::Utf8(Some(value)) = lit
                {
                    // Check for special value
                    if let Some(special) = F::handle_special(value) {
                        return Some(special);
                    }
                    return Some(F::from_single(value.clone()));
                }
                None
            }
            // Handle column IN ('a', 'b', 'c') or column NOT IN ('a', 'b')
            // Note: DataFusion often rewrites IN to OR, but keep this for completeness
            Expr::InList(in_list) => {
                if let Expr::Column(col) = in_list.expr.as_ref()
                    && col.name == self.column_name
                {
                    let values = extract_string_literals(in_list);
                    if !values.is_empty() {
                        // Handle NOT IN if supported
                        if in_list.negated {
                            if let Some(negated) = F::from_negated(values) {
                                return Some(negated);
                            }
                            return None;
                        }

                        // Check for special values
                        for value in &values {
                            if let Some(special) = F::handle_special(value) {
                                return Some(special);
                            }
                        }

                        if values.len() == 1 {
                            return Some(F::from_single(values.into_iter().next().unwrap()));
                        }
                        return Some(F::from_multiple(values));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Recursively collect values from an OR expression tree
    /// Returns true if all leaves are `column = 'value'` patterns
    fn collect_values_from_or(&self, expr: &Expr, values: &mut Vec<String>) -> bool {
        match expr {
            Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
                // Recursively collect from both sides
                self.collect_values_from_or(&binary.left, values)
                    && self.collect_values_from_or(&binary.right, values)
            }
            Expr::BinaryExpr(binary)
                if matches!(binary.op, Operator::Eq)
                    && matches!(binary.left.as_ref(), Expr::Column(col) if col.name == self.column_name) =>
            {
                // Extract the value
                if let Expr::Literal(lit, _) = binary.right.as_ref()
                    && let datafusion::common::ScalarValue::Utf8(Some(value)) = lit
                {
                    values.push(value.clone());
                    return true;
                }
                false
            }
            _ => false, // Not a valid pattern
        }
    }
}

/// Extract string literals from an IN list expression
fn extract_string_literals(in_list: &datafusion::logical_expr::expr::InList) -> Vec<String> {
    in_list
        .list
        .iter()
        .filter_map(|e| {
            if let Expr::Literal(lit, _) = e
                && let datafusion::common::ScalarValue::Utf8(Some(s)) = lit
            {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Deduplicate a list of strings while preserving order
fn deduplicate(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|v| seen.insert(v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{BinaryExpr, Expr, Operator, col, lit};

    // Simple filter type for testing
    #[derive(Debug, PartialEq, Eq)]
    enum TestFilter {
        Default,
        Single(String),
        Multiple(Vec<String>),
    }

    impl FilterValue for TestFilter {
        fn default() -> Self {
            TestFilter::Default
        }

        fn from_single(value: String) -> Self {
            TestFilter::Single(value)
        }

        fn from_multiple(values: Vec<String>) -> Self {
            TestFilter::Multiple(values)
        }
    }

    #[test]
    fn test_extract_single_value() {
        let extractor = FilterExtractor::<TestFilter>::new("test_col");
        let expr = col("test_col").eq(lit("value1"));

        let result = extractor.extract(&[expr]);
        assert_eq!(result, TestFilter::Single("value1".to_string()));
    }

    #[test]
    fn test_extract_no_match() {
        let extractor = FilterExtractor::<TestFilter>::new("test_col");
        let expr = col("other_col").eq(lit("value1"));

        let result = extractor.extract(&[expr]);
        assert_eq!(result, TestFilter::Default);
    }

    #[test]
    fn test_extract_from_or() {
        let extractor = FilterExtractor::<TestFilter>::new("test_col");
        // test_col = 'a' OR test_col = 'b'
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("test_col").eq(lit("a"))),
            op: Operator::Or,
            right: Box::new(col("test_col").eq(lit("b"))),
        });

        let result = extractor.extract(&[expr]);
        assert_eq!(
            result,
            TestFilter::Multiple(vec!["a".to_string(), "b".to_string()])
        );
    }
}
