package org.apache.spark.simple.rdd

import scala.reflect.ClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils
import org.apache.spark.simple.SparkEnv

/**
  * An implementation of checkpointing implemented on top of Spark's caching layer.
  *
  * Local checkpointing trades off fault tolerance for performance by skipping the expensive
  * step of saving the RDD data to a reliable and fault-tolerant storage. Instead, the data
  * is written to the local, ephemeral block storage that lives in each executor. This is useful
  * for use cases where RDDs build up long lineages that need to be truncated often (e.g. GraphX).
  */
private[spark] class LocalRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  /**
    * Ensure the RDD is fully cached so the partitions can be recovered later.
    */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel

    // Assume storage level uses disk; otherwise memory eviction may cause data loss
    assume(level.useDisk, s"Storage level $level is not appropriate for local checkpointing")

    // Not all actions compute all partitions of the RDD (e.g. take). For correctness, we
    // must cache any missing partitions. TODO: avoid running another job here (SPARK-8582).
    val action = (tc: TaskContext, iterator: Iterator[T]) => Utils.getIteratorSize(iterator)
    val missingPartitionIndices = rdd.partitions.map(_.index).filter { i =>
      !SparkEnv.get.blockManager.master.contains(RDDBlockId(rdd.id, i))
    }
    if (missingPartitionIndices.nonEmpty) {
      rdd.sparkContext.runJob(rdd, action, missingPartitionIndices)
    }

    new LocalCheckpointRDD[T](rdd)
  }

}

private[spark] object LocalRDDCheckpointData {

  val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

  /**
    * Transform the specified storage level to one that uses disk.
    *
    * This guarantees that the RDD can be recomputed multiple times correctly as long as
    * executors do not fail. Otherwise, if the RDD is cached in memory only, for instance,
    * the checkpoint data will be lost if the relevant block is evicted from memory.
    *
    * This method is idempotent.
    */
  def transformStorageLevel(level: StorageLevel): StorageLevel = {
    StorageLevel(useDisk = true, level.useMemory, level.deserialized, level.replication)
  }
}