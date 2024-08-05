//! Collector for part key binary data

use std::cmp::min;

use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::{BytesColumn, Column},
    TantivyError,
};

use crate::state::field_constants::{END_TIME, PART_KEY, START_TIME};

use super::column_cache::ColumnCache;

/// Records returned from queries
#[derive(Debug, PartialEq, Hash, PartialOrd, Eq)]
pub struct PartKeyRecord {
    pub part_key: Vec<u8>,
    pub start_time: i64,
    pub end_time: i64,
}

pub struct PartKeyRecordCollector {
    limit: usize,
    column_cache: ColumnCache,
}

impl PartKeyRecordCollector {
    pub fn new(limit: usize, column_cache: ColumnCache) -> Self {
        Self {
            limit,
            column_cache,
        }
    }
}

impl Collector for PartKeyRecordCollector {
    type Fruit = Vec<PartKeyRecord>;

    type Child = PartKeyRecordSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<PartKeyRecordSegmentCollector> {
        let part_key_column: BytesColumn =
            self.column_cache
                .get_bytes_column(segment, PART_KEY)?
                .ok_or_else(|| TantivyError::FieldNotFound(PART_KEY.to_string()))?;

        let start_time_column: Column<i64> = self
            .column_cache
            .get_column(segment, START_TIME)?
            .ok_or_else(|| TantivyError::FieldNotFound(START_TIME.to_string()))?;

        let end_time_column: Column<i64> = self
            .column_cache
            .get_column(segment, END_TIME)?
            .ok_or_else(|| TantivyError::FieldNotFound(END_TIME.to_string()))?;

        Ok(PartKeyRecordSegmentCollector {
            part_key_column,
            start_time_column,
            end_time_column,
            docs: Vec::new(),
            limit: self.limit,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<PartKeyRecord>>,
    ) -> tantivy::Result<Vec<PartKeyRecord>> {
        let len: usize = min(segment_fruits.iter().map(|x| x.len()).sum(), self.limit);

        let mut result = Vec::with_capacity(len);
        for part_ids in segment_fruits {
            result.extend(part_ids.into_iter().take(self.limit - result.len()));
        }

        Ok(result)
    }
}

pub struct PartKeyRecordSegmentCollector {
    part_key_column: BytesColumn,
    start_time_column: Column<i64>,
    end_time_column: Column<i64>,
    docs: Vec<PartKeyRecord>,
    limit: usize,
}

impl SegmentCollector for PartKeyRecordSegmentCollector {
    type Fruit = Vec<PartKeyRecord>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        if self.docs.len() >= self.limit {
            return;
        }

        let Some(ord) = self.part_key_column.ords().first(doc) else {
            return;
        };
        let mut part_key = vec![];
        if self
            .part_key_column
            .ord_to_bytes(ord, &mut part_key)
            .is_err()
        {
            return;
        }

        let Some(start_time) = self.start_time_column.first(doc) else {
            return;
        };

        let Some(end_time) = self.end_time_column.first(doc) else {
            return;
        };

        self.docs.push(PartKeyRecord {
            part_key,
            start_time,
            end_time,
        });
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tantivy::query::AllQuery;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[test]
    fn test_part_key_collector() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = PartKeyRecordCollector::new(usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs, IDs 1 and 10
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            [
                PartKeyRecord {
                    part_key: vec![0x41, 0x41],
                    start_time: 1234,
                    end_time: 1235
                },
                PartKeyRecord {
                    part_key: vec![0x42, 0x42],
                    start_time: 4321,
                    end_time: 10000
                }
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_part_key_collector_with_limit() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = PartKeyRecordCollector::new(1, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.len(), 1);
    }
}
