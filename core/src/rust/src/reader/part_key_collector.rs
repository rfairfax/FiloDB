//! Collector for part key binary data

use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::BytesColumn,
    TantivyError,
};

use crate::state::field_constants;

use super::column_cache::ColumnCache;

pub struct PartKeyCollector {
    column_cache: ColumnCache,
}

impl PartKeyCollector {
    pub fn new(column_cache: ColumnCache) -> Self {
        Self { column_cache }
    }
}

impl Collector for PartKeyCollector {
    type Fruit = Option<Vec<u8>>;

    type Child = PartKeySegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<PartKeySegmentCollector> {
        let part_key_column: BytesColumn = self
            .column_cache
            .get_bytes_column(segment, field_constants::PART_KEY)?
            .ok_or_else(|| TantivyError::FieldNotFound(field_constants::PART_KEY.to_string()))?;

        Ok(PartKeySegmentCollector {
            part_key_column,
            doc: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Option<Vec<u8>>>,
    ) -> tantivy::Result<Option<Vec<u8>>> {
        Ok(segment_fruits.into_iter().flatten().next())
    }
}

pub struct PartKeySegmentCollector {
    part_key_column: BytesColumn,
    doc: Option<Vec<u8>>,
}

impl SegmentCollector for PartKeySegmentCollector {
    type Fruit = Option<Vec<u8>>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        if self.doc.is_some() {
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

        self.doc = Some(part_key);
    }

    fn harvest(self) -> Self::Fruit {
        self.doc
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{
        query::{EmptyQuery, TermQuery},
        schema::IndexRecordOption,
        Term,
    };

    use crate::test_utils::{build_test_schema, COL1_NAME};

    use super::*;

    #[test]
    fn test_part_key_collector() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = PartKeyCollector::new(column_cache);
        let query = TermQuery::new(
            Term::from_field_text(index.schema.get_field(COL1_NAME).unwrap(), "ABC"),
            IndexRecordOption::Basic,
        );

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(results, Some(vec![0x41, 0x41]));
    }

    #[test]
    fn test_part_key_collector_no_match() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = PartKeyCollector::new(column_cache);

        let query = EmptyQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results, None);
    }
}
