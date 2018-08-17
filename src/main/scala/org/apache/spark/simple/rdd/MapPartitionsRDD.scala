package org.apache.spark.simple.rdd

import org.apache.spark.Partition
import org.apache.spark.simple.TaskContext.TaskContext

import scala.reflect.ClassTag

/**
  * An RDD that applies the provided function to every partition of the parent RDD.
  */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
                                                                 var prev: RDD[T],
                                                                 f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
                                                                 preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
