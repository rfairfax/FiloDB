//! State objects shared with Java

use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use jni::sys::jlong;
use quick_cache::sync::Cache;
use tantivy::{
    collector::{Collector, SegmentCollector},
    query::{EnableScoring, Weight},
    schema::{Field, OwnedValue, Schema},
    IndexReader, IndexWriter, SegmentId, TantivyDocument,
};
use tantivy_common::BitSet;

use crate::{
    errors::JavaResult,
    query::{
        bitset_weight::BitSetWeight,
        cache::{CachableQuery, CachableQueryKey, CachableQueryWeighter},
    },
    reader::column_cache::ColumnCache,
};

pub struct IndexHandle {
    // Fields that don't need explicit synchronization
    //
    //
    // Schema for this nidex
    pub schema: Schema,
    // Default field for JSON searches
    pub default_field: Option<Field>,
    // Active reader
    pub reader: IndexReader,
    // Cache of query -> docs
    cache: Cache<(SegmentId, CachableQuery), Arc<BitSet>, CachableQueryWeighter>,
    // Are there changes pending to commit
    pub changes_pending: AtomicBool,
    // Column lookup cache
    pub column_cache: ColumnCache,

    // Fields that need synchronization
    //
    //
    // Active writer
    pub writer: RwLock<IndexWriter>,
}

// Tuning parameters for query cache
const QUERY_CACHE_MAX_SIZE_BYTES: u64 = 50_000_000;
// Rough estimate of bitset size - 250k docs
const QUERY_CACHE_AVG_ITEM_SIZE: u64 = 31250;
const QUERY_CACHE_ESTIMATED_ITEM_COUNT: u64 =
    QUERY_CACHE_MAX_SIZE_BYTES / QUERY_CACHE_AVG_ITEM_SIZE;

impl IndexHandle {
    pub fn new_handle(
        schema: Schema,
        default_field: Option<Field>,
        writer: IndexWriter,
        reader: IndexReader,
    ) -> jlong {
        let obj = Box::new(Self {
            schema,
            default_field,
            writer: RwLock::new(writer),
            reader,
            changes_pending: AtomicBool::new(false),
            cache: Cache::with_weighter(
                QUERY_CACHE_ESTIMATED_ITEM_COUNT as usize,
                QUERY_CACHE_MAX_SIZE_BYTES,
                CachableQueryWeighter,
            ),
            column_cache: ColumnCache::new(),
        });

        Box::into_raw(obj) as jlong
    }

    /// Decode handle back into a reference
    pub fn get_ref_from_handle<'a>(handle: jlong) -> &'a Self {
        let ptr = handle as *const IndexHandle;

        unsafe { &*ptr }
    }

    pub fn query_cache_stats(&self) -> (u64, u64) {
        (self.cache.hits(), self.cache.misses())
    }

    /// Execute a cachable query
    pub fn execute_cachable_query<C>(
        &self,
        cachable_query: CachableQuery,
        collector: C,
    ) -> JavaResult<C::Fruit>
    where
        C: Collector,
    {
        let searcher = self.reader.searcher();
        let scoring = EnableScoring::disabled_from_searcher(&searcher);

        let mut query_weight: Option<Box<dyn Weight>> = None;

        let segment_readers = searcher.segment_readers();
        let mut fruits: Vec<<C::Child as SegmentCollector>::Fruit> =
            Vec::with_capacity(segment_readers.len());

        // Note - the query optimizations here only work for the single threaded querying.  That matches
        // the pattern FiloDB uses because it will dispatch multiple queries at a time on different threads,
        // so this results in net improvement anyway.  If we need to change to the multithreaded executor
        // in the future then the lazy query evaluation code will need some work
        for (segment_ord, segment_reader) in segment_readers.iter().enumerate() {
            // Is it cached
            let cache_key = CachableQueryKey(segment_reader.segment_id(), &cachable_query);

            let docs = if let Some(docs) = self.cache.get(&cache_key) {
                // Cache hit
                docs
            } else {
                // Build query if needed.  We do this lazily as it may be expensive to parse a regex, for example.
                // This can give a 2-4x speedup in some cases.
                let weight = if let Some(weight) = &query_weight {
                    weight
                } else {
                    let query = cachable_query.to_query(&self.schema, self.default_field)?;
                    let weight = query.weight(scoring)?;

                    query_weight = Some(weight);

                    // Unwrap is safe here because we just set the value
                    #[allow(clippy::unwrap_used)]
                    query_weight.as_ref().unwrap()
                };

                // Load bit set
                let mut bitset = BitSet::with_max_value(segment_reader.max_doc());

                weight.for_each_no_score(segment_reader, &mut |docs| {
                    for doc in docs.iter().cloned() {
                        bitset.insert(doc);
                    }
                })?;

                let bitset = Arc::new(bitset);

                if cachable_query.should_cache() {
                    self.cache.insert(cache_key.into(), bitset.clone());
                }

                bitset
            };

            let weight = BitSetWeight::new(docs);
            let results = collector.collect_segment(&weight, segment_ord as u32, segment_reader)?;

            fruits.push(results);
        }

        Ok(collector.merge_fruits(fruits)?)
    }
}

/// A document that is actively being built up for ingesting
#[derive(Default)]
pub struct IngestingDocument {
    // List of map entries we're building up to store in the document
    pub map_values: HashMap<String, BTreeMap<String, OwnedValue>>,
    // List of field names in the document being ingested
    pub field_names: Vec<String>,
    // Document state for ingestion
    pub doc: TantivyDocument,
}

pub mod field_constants {
    pub fn facet_field_name(name: &str) -> String {
        format!("{}{}", FACET_FIELD_PREFIX, name)
    }

    // These should be kept in sync with the constants in  PartKeyIndex.scala
    // as they're fields that can be directly queried via incoming filters
    // or fields that are filtered out of label lists
    pub const DOCUMENT_ID: &str = "__partIdField__";
    pub const PART_ID: &str = "__partIdDv__";
    pub const PART_KEY: &str = "__partKey__";
    pub const LABEL_LIST: &str = "__labelList__";
    pub const FACET_FIELD_PREFIX: &str = "$facet_";
    pub const START_TIME: &str = "__startTime__";
    pub const END_TIME: &str = "__endTime__";
    pub const TYPE: &str = "_type_";
}
