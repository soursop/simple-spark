package org.apache.spark.simple.rdd

import org.apache.spark.Partition
import org.apache.spark.simple.SparkContext
import org.apache.spark.simple.TaskContext.TaskContext

import scala.reflect.ClassTag

/**
  * An RDD partition used to recover checkpointed data.
  */
private[spark] class CheckpointRDDPartition(val index: Int) extends Partition

/**
  * An RDD that recovers checkpointed data from storage.
  */
private[spark] abstract class CheckpointRDD[T: ClassTag](sc: SparkContext)
  extends RDD[T](sc, Nil) {

  // CheckpointRDD should not be checkpointed again
  override def doCheckpoint(): Unit = { }
  override def checkpoint(): Unit = { }
  override def localCheckpoint(): this.type = this

  // Note: There is a bug in MiMa that complains about `AbstractMethodProblem`s in the
  // base [[org.apache.spark.rdd.RDD]] class if we do not override the following methods.
  // scalastyle:off
  protected override def getPartitions: Array[Partition] = ???
  override def compute(p: Partition, tc: TaskContext): Iterator[T] = ???
  // scalastyle:on

}