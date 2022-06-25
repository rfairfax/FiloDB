package filodb.core.memstore

import kamon.Kamon
import kamon.metric.MeasurementUnit
import monix.eval.Task
import monix.reactive.Observable

import filodb.core.DatasetRef
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.Schemas
import filodb.core.store.{ColumnStore, PartKeyRecord}

class IndexBootstrapper(colStore: ColumnStore) {

  /**
   * Bootstrap the lucene index for the shard
   * using PartKeyRecord objects read from some persistent source.
   *
   * The partId used in the lucene index is generated by invoking
   * the function provided on the threadpool requested.
   *
   * @param index the lucene index to populate
   * @param shardNum shard number
   * @param ref dataset ref
   * @param assignPartId the function to invoke to get the partitionId to be used to populate the index record
   * @return number of updated records
   */
  def bootstrapIndexRaw(index: PartKeyLuceneIndex,
                        shardNum: Int,
                        ref: DatasetRef)
                       (assignPartId: PartKeyRecord => Int): Task[Long] = {

    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    colStore.scanPartKeys(ref, shardNum)
      .map { pk =>
        val partId = assignPartId(pk)
        index.addPartKey(pk.partKey, partId, pk.startTime, pk.endTime)()
      }
      .countL
      .map { count =>
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }

  // TODO
  // currently we cannot yet recover from a particular checkpoint, look at the comment of the method
  // recoverIndexInternal() of DownsampleTimeSeriesShard. We, however, already have a checkpointer manager in place,
  // so, when we fix IndexBootstrapper, the checkpointMillis passed into this method can be utilized. Currently,
  // checkpointMillis passed to this method would always be None, as checkpointing logic can be activating only using
  // properties index-location and index-metastore-implementation and these are only utilized while testing
  // persistent index logic in dev.
  /**
   * Same as bootstrapIndexRaw, except that we parallelize lucene update for
   * faster bootstrap of large number of index entries in downsample cluster.
   * Not doing this in raw cluster since parallel TimeSeriesPartition
   * creation requires more careful contention analysis
   */
  def bootstrapIndexDownsample(index: PartKeyLuceneIndex,
                               shardNum: Int,
                               ref: DatasetRef,
                               checkpointMillis: Option[Long],
                               ttlMs: Long)
                              (assignPartId: PartKeyRecord => Int): Task[Long] = {

    // This is where we need to only get the delta from  PartKeyLuceneIndex and fetch part keys updated after
    // the timestamp in millis, based on the time, we need to invoke refreshWithDownsamplePartKeys giving
    // the last synced time till current hour, the start hour will be max(of the time retrieved from underlying
    // state, start - ttlMs)
    val recoverIndexLatency = Kamon.gauge("shard-recover-index-latency", MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    // here we need to adjust ttlMs
    // because we might already have index available on disk
    var checkpointTime = checkpointMillis.getOrElse(0L)
    if (checkpointTime < start - ttlMs) {
      checkpointTime = start - ttlMs
    }
    // No need to check index state here, by definition bootstrap index
    // will refresh the entire index. When entire index is rebuilt, the
    // partIds (numeric values) which are a part of the index, monotocally
    // increases. Bootstrap method scans all the part keys and treats them
    // as opaque byte arrays. The assignPartId function is cheap as it simply increments
    // a numeric value. This is ok since the index starts as a clean slate and thus the part ids too
    // start from 0. If index for a given range is desired, refreshWithDownsamplePartKeys should be called
    index.notifyLifecycleListener(IndexState.Refreshing, checkpointTime)
    colStore.scanPartKeys(ref, shardNum)
      .filter(_.endTime > checkpointTime)
      .mapParallelUnordered(Runtime.getRuntime.availableProcessors()) { pk =>
        Task.evalAsync {
          val partId = assignPartId(pk)
          index.addPartKey(pk.partKey, partId, pk.startTime, pk.endTime)()
        }
      }
      .countL
      .map { count =>
        // Ensures index is made durable to secondary store
        index.commit()
        // Note that we do not set an end time for the Synced here, instead
        // we will do it from DownsampleTimeSeriesShard
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }

  /**
   * Refresh index with real-time data rom colStore's raw dataset
   * @param fromHour fromHour inclusive
   * @param toHour toHour inclusive
   * @param parallelism number of threads to use to concurrently load the index
   * @param lookUpOrAssignPartId function to invoke to assign (or lookup) partId to the partKey
   *
   * @return number of records refreshed
   */
  def refreshWithDownsamplePartKeys(
                                     index: PartKeyLuceneIndex,
                                     shardNum: Int,
                                     ref: DatasetRef,
                                     fromHour: Long,
                                     toHour: Long,
                                     schemas: Schemas,
                                     parallelism: Int = Runtime.getRuntime.availableProcessors())
                                   (lookUpOrAssignPartId: Array[Byte] => Int): Task[Long] = {

    // This method needs to be invoked for updating a range of time in an existing index. This assumes the
    // Index is already present and we need to update some partKeys in it. The lookUpOrAssignPartId is expensive
    // The part keys byte array unlike in bootstrapIndexDownsample is not opaque. The part key is broken down into
    // key value pairs, looked up in index to find the already assigned partId if any. If no partId is found the next
    // available value from the counter in DownsampleTimeSeriesShard is allocated. However, since partId is an integer
    // the max value it can reach is 2^32. This is a lot of timeseries in one shard, however, with time, especially in
    // case of durable index, and in environments with high churn, partIds evicted are not reclaimed and we may
    // potentially exceed the limit requiring us to preiodically reclaim partIds, eliminate the notion of partIds or
    // comeup with alternate solutions to come up a partId which can either be a long value or some string
    // representation
    val recoverIndexLatency = Kamon.gauge("downsample-store-refresh-index-latency",
      MeasurementUnit.time.milliseconds)
      .withTag("dataset", ref.dataset)
      .withTag("shard", shardNum)
    val start = System.currentTimeMillis()
    index.notifyLifecycleListener(IndexState.Refreshing, fromHour * 3600 * 1000L)
    Observable.fromIterable(fromHour to toHour).flatMap { hour =>
      colStore.getPartKeysByUpdateHour(ref, shardNum, hour)
    }.mapParallelUnordered(parallelism) { pk =>
      // Same PK can be updated multiple times, but they wont be close for order to matter.
      // Hence using mapParallelUnordered
      Task.evalAsync {
        val downsamplePartKey = RecordBuilder.buildDownsamplePartKey(pk.partKey, schemas)
        downsamplePartKey.foreach { dpk =>
          val partId = lookUpOrAssignPartId(dpk)
          index.upsertPartKey(dpk, partId, pk.startTime, pk.endTime)()
        }
      }
    }
      .countL
      .map { count =>
        // Forces sync with underlying filesystem, on problem is for initial index sync
        // its all or nothing as we do not mark partial progress, but given the index
        // update is parallel it makes sense to wait for all to be added to index
        index.commit()
        index.notifyLifecycleListener(IndexState.Synced, toHour * 3600 * 1000L)
        index.refreshReadersBlocking()
        recoverIndexLatency.update(System.currentTimeMillis() - start)
        count
      }
  }
}

