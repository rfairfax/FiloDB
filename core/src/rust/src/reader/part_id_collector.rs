//! Collector to pull part IDs from a document

use std::cmp::min;

use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    TantivyError,
};

use crate::state::field_constants;

use super::column_cache::ColumnCache;

pub struct PartIdCollector {
    limit: usize,
    column_cache: ColumnCache,
}

impl PartIdCollector {
    pub fn new(limit: usize, column_cache: ColumnCache) -> Self {
        Self {
            limit,
            column_cache,
        }
    }
}

impl Collector for PartIdCollector {
    type Fruit = Vec<i32>;

    type Child = PartIdSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<PartIdSegmentCollector> {
        let column: Column<i64> = self
            .column_cache
            .get_column(segment, field_constants::PART_ID)?
            .ok_or_else(|| TantivyError::FieldNotFound(field_constants::PART_ID.to_string()))?;

        Ok(PartIdSegmentCollector {
            column,
            docs: Vec::new(),
            limit: self.limit,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segment_fruits: Vec<Vec<i32>>) -> tantivy::Result<Vec<i32>> {
        let len: usize = min(segment_fruits.iter().map(|x| x.len()).sum(), self.limit);

        let mut result = Vec::with_capacity(len);
        for part_ids in segment_fruits {
            result.extend(part_ids.iter().take(self.limit - result.len()));
        }

        Ok(result)
    }
}

pub struct PartIdSegmentCollector {
    column: Column<i64>,
    docs: Vec<i32>,
    limit: usize,
}

impl SegmentCollector for PartIdSegmentCollector {
    type Fruit = Vec<i32>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        if self.docs.len() >= self.limit {
            return;
        }

        if let Some(val) = self.column.first(doc) {
            self.docs.push(val as i32);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashSet;
    use tantivy::query::AllQuery;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[test]
    fn test_part_id_collector() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let collector = PartIdCollector::new(usize::MAX, cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs, IDs 1 and 10
        assert_eq!(
            results.into_iter().collect::<HashSet<i32>>(),
            [1, 10].into_iter().collect::<HashSet<i32>>()
        );
    }

    #[test]
    fn test_part_id_collector_with_limit() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let collector = PartIdCollector::new(1, cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.len(), 1);
    }
}
