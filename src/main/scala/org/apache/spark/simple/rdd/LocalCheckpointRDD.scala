package org.apache.spark.simple.rdd

import org.apache.spark.{Partition, SparkException}
import org.apache.spark.simple.SparkContext
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.storage.RDDBlockId

import scala.reflect.ClassTag

/**
  * A dummy CheckpointRDD that exists to provide informative error messages during failures.
  *
  * This is simply a placeholder because the original checkpointed RDD is expected to be
  * fully cached. Only if an executor fails or if the user explicitly unpersists the original
  * RDD will Spark ever attempt to compute this CheckpointRDD. When this happens, however,
  * we must provide an informative error message.
  *
  * @param sc the active SparkContext
  * @param rddId the ID of the checkpointed RDD
  * @param numPartitions the number of partitions in the checkpointed RDD
  */
private[spark] class LocalCheckpointRDD[T: ClassTag](
                                                      sc: SparkContext,
                                                      rddId: Int,
                                                      numPartitions: Int)
  extends CheckpointRDD[T](sc) {

  def this(rdd: RDD[T]) {
    this(rdd.context, rdd.id, rdd.partitions.length)
  }

  protected override def getPartitions: Array[Partition] = {
    (0 until numPartitions).toArray.map { i => new CheckpointRDDPartition(i) }
  }

  /**
    * Throw an exception indicating that the relevant block is not found.
    *
    * This should only be called if the original RDD is explicitly unpersisted or if an
    * executor is lost. Under normal circumstances, however, the original RDD (our child)
    * is expected to be fully cached and so all partitions should already be computed and
    * available in the block storage.
    */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    throw new SparkException(
      s"Checkpoint block ${RDDBlockId(rddId, partition.index)} not found! Either the executor " +
        s"that originally checkpointed this partition is no longer alive, or the original RDD is " +
        s"unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` " +
        s"instead, which is slower than local checkpointing but more fault-tolerant.")
  }

}

