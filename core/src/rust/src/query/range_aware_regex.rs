//! Range aware Regex query

use std::sync::Arc;

use tantivy::{
    query::{AutomatonWeight, EnableScoring, Query, Weight},
    schema::Field,
    TantivyError,
};
use tantivy_fst::{Automaton, Regex};

use super::JSON_PREFIX_SEPARATOR;

// Tantivy's inbox RegexQuery looks at all possible dictionary values for matches
// For JSON fields this means looking at a lot of values for other fields that can never match
// This class is range aware limiting the number of considered terms

#[derive(Debug, Clone)]
pub struct RangeAwareRegexQuery {
    regex: Arc<SkipPrefixAutomaton<Regex>>,
    prefix: String,
    field: Field,
}

impl RangeAwareRegexQuery {
    /// Creates a new RegexQuery from a given pattern
    pub fn from_pattern(
        regex_pattern: &str,
        prefix: &str,
        field: Field,
    ) -> Result<Self, TantivyError> {
        let regex = Regex::new(regex_pattern).map_err(|err| {
            TantivyError::InvalidArgument(format!("RanageAwareRegexQuery: {err}"))
        })?;

        let regex = SkipPrefixAutomaton {
            inner: regex,
            prefix_size: if prefix.is_empty() {
                0
            } else {
                prefix.len() + JSON_PREFIX_SEPARATOR.len()
            },
        };

        Ok(RangeAwareRegexQuery {
            regex: regex.into(),
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{}\0s", prefix)
            },
            field,
        })
    }

    fn specialized_weight(&self) -> AutomatonWeight<SkipPrefixAutomaton<Regex>> {
        if self.prefix.is_empty() {
            AutomatonWeight::new(self.field, self.regex.clone())
        } else {
            AutomatonWeight::new_for_json_path(
                self.field,
                self.regex.clone(),
                self.prefix.as_bytes(),
            )
        }
    }
}

impl Query for RangeAwareRegexQuery {
    fn weight(&self, _enabled_scoring: EnableScoring<'_>) -> Result<Box<dyn Weight>, TantivyError> {
        Ok(Box::new(self.specialized_weight()))
    }
}

#[derive(Debug)]
pub struct SkipPrefixAutomaton<A> {
    inner: A,
    prefix_size: usize,
}

#[derive(Clone)]
pub struct SkipPrefixAutomatonState<A> {
    count: usize,
    inner: A,
}

impl<A> Automaton for SkipPrefixAutomaton<A>
where
    A: Automaton,
    A::State: Clone,
{
    type State = SkipPrefixAutomatonState<A::State>;

    fn start(&self) -> Self::State {
        Self::State {
            count: 0,
            inner: self.inner.start(),
        }
    }

    fn is_match(&self, state: &Self::State) -> bool {
        if state.count < self.prefix_size {
            false
        } else {
            self.inner.is_match(&state.inner)
        }
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        let mut state = state.clone();

        if state.count < self.prefix_size {
            state.count += 1
        } else {
            state.inner = self.inner.accept(&state.inner, byte);
        };

        state
    }

    fn can_match(&self, state: &Self::State) -> bool {
        if state.count < self.prefix_size {
            true
        } else {
            self.inner.can_match(&state.inner)
        }
    }

    fn will_always_match(&self, state: &Self::State) -> bool {
        if state.count < self.prefix_size {
            false
        } else {
            self.inner.will_always_match(&state.inner)
        }
    }
}
