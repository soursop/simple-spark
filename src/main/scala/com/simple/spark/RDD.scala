package com.simple.spark

import com.simple.spark.TaskContext.TaskContext
import org.apache.spark.internal.Logging

abstract class RDD[T](@transient private var _sc: SparkContext) extends Serializable with Logging {
  def compute(split: Partition, context: TaskContext): Iterator[T]
}
