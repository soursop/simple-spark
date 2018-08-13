package com.simple.spark

import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.{SerializableWritable, SparkContext}


/**
  * A Spark split class that wraps around a Hadoop InputSplit.
  */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

class HadoopRDD[K, V](sc: SparkContext) extends RDD[(K, V)](sc) {
  override def compute(split: Partition, context: TaskContext.TaskContext): Iterator[(K, V)] = ???
}
