//! Methods related to reading / querying the index

use std::sync::atomic::Ordering;

use hashbrown::HashSet;
use jni::{
    objects::{JByteArray, JClass, JIntArray, JObject, JString, JValue},
    sys::{jbyteArray, jint, jintArray, jlong, jlongArray, jobjectArray},
    JNIEnv,
};
use part_id_collector::PartIdCollector;
use part_key_collector::PartKeyCollector;
use part_key_record_collector::PartKeyRecordCollector;
use string_field_collector::StringFieldCollector;
use tantivy::{collector::FacetCollector, schema::FieldType};
use time_collector::TimeCollector;

use crate::{
    errors::{JavaException, JavaResult},
    exec::jni_exec,
    index::WRITER_MEM_BUDGET,
    jnienv::JNIEnvExt,
    query::cache::CachableQuery,
    state::{
        field_constants::{self, facet_field_name},
        IndexHandle,
    },
};

pub mod column_cache;
mod part_id_collector;
mod part_key_collector;
mod part_key_record_collector;
mod string_field_collector;
mod time_collector;

const PART_KEY_RECORD_CLASS: &str = "filodb/core/memstore/PartKeyLuceneIndexRecord";
const TERM_INFO_CLASS: &str = "filodb/core/memstore/TermInfo";
const UTF8STR_CLASS: &str = "filodb/memory/format/ZeroCopyUTF8String";

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_indexRamBytes(
    mut env: JNIEnv,
    _class: JClass,
    _handle: jlong,
) -> jlong {
    jni_exec(&mut env, |_| Ok(WRITER_MEM_BUDGET as jlong))
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_refreshReaders(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);
        {
            let changes_pending = handle.changes_pending.swap(false, Ordering::SeqCst);

            if changes_pending {
                let mut writer = handle.writer.write()?;
                writer.commit()?;
            }

            handle.reader.reload()?;
        };

        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_indexNumEntries(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);
        let searcher = handle.reader.searcher();

        Ok(searcher.num_docs() as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_partIdsEndedBefore(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    ended_before: jlong,
) -> jintArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let query = CachableQuery::ByEndTime(ended_before);
        let collector = PartIdCollector::new(usize::MAX, handle.column_cache.clone());

        let results = handle.execute_cachable_query(query, collector)?;

        let java_ret = env.new_int_array(results.len() as i32)?;
        env.set_int_array_region(&java_ret, 0, &results)?;

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_partIdFromPartKey(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    part_id: JByteArray,
) -> jint {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let bytes = env.get_byte_array(&part_id)?;

        let query = CachableQuery::ByPartKey(bytes.into_boxed_slice().into());

        let collector = PartIdCollector::new(1, handle.column_cache.clone());
        let results = handle
            .execute_cachable_query(query, collector)?
            .into_iter()
            .next();

        let result = results.unwrap_or(-1);

        Ok(result)
    })
}

fn fetch_label_names(
    query: CachableQuery,
    handle: &IndexHandle,
    results: &mut HashSet<String>,
) -> JavaResult<()> {
    // Get LABEL_NAMES facet
    let mut collector = FacetCollector::for_field(facet_field_name(field_constants::LABEL_LIST));
    collector.add_facet("/");

    let query_results = handle.execute_cachable_query(query, collector)?;
    let query_results: Vec<_> = query_results.get("/").collect();
    for (facet, _count) in query_results {
        results.insert(facet.to_path()[0].to_string());
    }

    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_labelNames(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query: JByteArray,
    limit: jint,
) -> jobjectArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let mut results = HashSet::new();

        let query_bytes = env.get_byte_array(&query)?;

        let query = CachableQuery::Complex(query_bytes.into_boxed_slice().into());
        fetch_label_names(query, handle, &mut results)?;

        let len = std::cmp::min(results.len(), limit as usize);

        let java_ret = env.new_object_array(len as i32, "java/lang/String", JObject::null())?;
        for (i, item) in results.drain().take(len).enumerate() {
            env.set_object_array_element(&java_ret, i as i32, env.new_string(item)?)?;
        }

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_indexNames(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jobjectArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let mut results = HashSet::new();

        // For each indexed field, include it
        // For map fields, include encoded sub fields
        for (_field, field_entry) in handle.schema.fields() {
            match field_entry.field_type() {
                FieldType::JsonObject(..) => {
                    // Skip this, we're going to get subfields via the facet below
                }
                _ => {
                    results.insert(field_entry.name().to_string());
                }
            };
        }

        let query = CachableQuery::All;
        fetch_label_names(query, handle, &mut results)?;

        let java_ret =
            env.new_object_array(results.len() as i32, "java/lang/String", JObject::null())?;
        for (i, item) in results.drain().enumerate() {
            env.set_object_array_element(&java_ret, i as i32, env.new_string(item)?)?;
        }

        Ok(java_ret.into_raw())
    })
}

const MAX_TERMS_TO_ITERATE: usize = 10_000;

fn query_label_values(
    query: CachableQuery,
    handle: &IndexHandle,
    mut field: String,
    limit: usize,
    term_limit: usize,
) -> JavaResult<Vec<(String, u64)>> {
    let field_and_prefix = handle
        .schema
        .find_field_with_default(&field, handle.default_field);

    if let Some((f, prefix)) = field_and_prefix {
        if !prefix.is_empty() {
            let field_name = handle.schema.get_field_entry(f).name();
            field = format!("{}.{}", field_name, prefix);
        }

        let collector =
            StringFieldCollector::new(&field, limit, term_limit, handle.column_cache.clone());
        Ok(handle.execute_cachable_query(query, collector)?)
    } else {
        // Invalid field, no values
        Ok(vec![])
    }
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_labelValues(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query: JByteArray,
    field: JString,
    top_k: jint,
) -> jobjectArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let top_k = top_k as usize;

        let field = env.get_rust_string(&field)?;

        let query_bytes = env.get_byte_array(&query)?;

        let query = CachableQuery::Complex(query_bytes.into_boxed_slice().into());

        let results = query_label_values(query, handle, field, top_k, usize::MAX)?;

        let results_len = std::cmp::min(top_k, results.len());
        let java_ret =
            env.new_object_array(results_len as i32, "java/lang/String", JObject::null())?;
        for (i, (value, _)) in results.into_iter().take(top_k).enumerate() {
            let term_str = env.new_string(&value)?;
            env.set_object_array_element(&java_ret, i as i32, &term_str)?;
        }

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_indexValues(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    field: JString,
    top_k: jint,
) -> jobjectArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let top_k = top_k as usize;

        let field = env.get_rust_string(&field)?;

        let query = CachableQuery::All;
        let results = query_label_values(
            query,
            handle,
            field,
            MAX_TERMS_TO_ITERATE,
            MAX_TERMS_TO_ITERATE,
        )?;

        let results_len = std::cmp::min(top_k, results.len());
        let java_ret =
            env.new_object_array(results_len as i32, TERM_INFO_CLASS, JObject::null())?;
        for (i, (value, count)) in results.into_iter().take(top_k).enumerate() {
            let len = value.as_bytes().len();
            let term_bytes = env.new_byte_array(len as i32)?;
            let bytes_ptr = value.as_bytes().as_ptr() as *const i8;
            let bytes_ptr = unsafe { std::slice::from_raw_parts(bytes_ptr, len) };

            env.set_byte_array_region(&term_bytes, 0, bytes_ptr)?;

            let term_str = env
                .call_static_method(
                    UTF8STR_CLASS,
                    "apply",
                    "([B)Lfilodb/memory/format/ZeroCopyUTF8String;",
                    &[JValue::Object(&term_bytes)],
                )?
                .l()?;

            let term_info_obj = env.new_object(
                TERM_INFO_CLASS,
                "(Lfilodb/memory/format/ZeroCopyUTF8String;I)V",
                &[JValue::Object(&term_str), JValue::Int(count as i32)],
            )?;
            env.set_object_array_element(&java_ret, i as i32, &term_info_obj)?;
        }

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_queryPartIds(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query: JByteArray,
    limit: jint,
) -> jintArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let query_bytes = env.get_byte_array(&query)?;

        let query = CachableQuery::Complex(query_bytes.into_boxed_slice().into());

        let collector = PartIdCollector::new(limit as usize, handle.column_cache.clone());
        let results = handle.execute_cachable_query(query, collector)?;

        let java_ret = env.new_int_array(results.len() as i32)?;
        env.set_int_array_region(&java_ret, 0, &results)?;

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_queryPartKeyRecords(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query: JByteArray,
    limit: jint,
) -> jobjectArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let query_bytes = env.get_byte_array(&query)?;

        let query = CachableQuery::Complex(query_bytes.into_boxed_slice().into());

        let collector = PartKeyRecordCollector::new(limit as usize, handle.column_cache.clone());
        let results = handle.execute_cachable_query(query, collector)?;

        let java_ret =
            env.new_object_array(results.len() as i32, PART_KEY_RECORD_CLASS, JObject::null())?;
        for (i, result) in results.into_iter().enumerate() {
            let bytes_obj = env.new_byte_array(result.part_key.len() as i32)?;
            let bytes_ptr = result.part_key.as_ptr() as *const i8;
            let bytes_ptr = unsafe { std::slice::from_raw_parts(bytes_ptr, result.part_key.len()) };

            env.set_byte_array_region(&bytes_obj, 0, bytes_ptr)?;

            let result_obj = env.new_object(
                PART_KEY_RECORD_CLASS,
                "([BJJ)V",
                &[
                    JValue::Object(&bytes_obj),
                    JValue::Long(result.start_time),
                    JValue::Long(result.end_time),
                ],
            )?;
            env.set_object_array_element(&java_ret, i as i32, result_obj)?;
        }

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_queryPartKey(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query: JByteArray,
    limit: jint,
) -> jbyteArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        if limit != 1 {
            return Err(JavaException::new_runtime_exception(
                "Only limit of 1 is supported for queryPartKey",
            ));
        }

        let query_bytes = env.get_byte_array(&query)?;
        let query = CachableQuery::Complex(query_bytes.into_boxed_slice().into());

        let collector = PartKeyCollector::new(handle.column_cache.clone());
        let results = handle.execute_cachable_query(query, collector)?;

        let java_ret = match results {
            Some(part_key) => {
                let bytes_obj = env.new_byte_array(part_key.len() as i32)?;
                let bytes_ptr = part_key.as_ptr() as *const i8;
                let bytes_ptr = unsafe { std::slice::from_raw_parts(bytes_ptr, part_key.len()) };

                env.set_byte_array_region(&bytes_obj, 0, bytes_ptr)?;

                bytes_obj.into_raw()
            }
            None => JObject::null().into_raw(),
        };

        Ok(java_ret)
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_startTimeFromPartIds(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    part_ids: JIntArray,
) -> jlongArray {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let len = env.get_array_length(&part_ids)?;

        let mut part_id_values = vec![0i32; len as usize];
        env.get_int_array_region(&part_ids, 0, &mut part_id_values[..])?;

        let query = CachableQuery::ByPartIds(part_id_values.into_boxed_slice().into());

        let collector = TimeCollector::new(
            field_constants::START_TIME,
            usize::MAX,
            handle.column_cache.clone(),
        );

        let results = handle.execute_cachable_query(query, collector)?;

        // Return is encoded as a single long array of tuples of part id, start time repeated. For example
        // the first part ID is at offset 0, then its start time is at offset 1, the next part id is at offset 2
        // and its start time is at offset 3, etc.
        //
        // This lets us avoid non primitive types in the return, which greatly improves performance
        let java_ret = env.new_long_array(results.len() as i32 * 2)?;
        let mut local_array = Vec::with_capacity(results.len() * 2);

        for (p, t) in results {
            local_array.push(p as i64);
            local_array.push(t);
        }

        env.set_long_array_region(&java_ret, 0, &local_array)?;

        Ok(java_ret.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_endTimeFromPartId(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    part_id: jint,
) -> jlong {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let query = CachableQuery::ByPartId(part_id);

        let collector =
            TimeCollector::new(field_constants::END_TIME, 1, handle.column_cache.clone());

        let results = handle.execute_cachable_query(query, collector)?;

        let result = results
            .into_iter()
            .next()
            .map(|(_id, time)| time)
            .unwrap_or(-1);

        Ok(result)
    })
}
