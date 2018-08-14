package org.apache.spark.simple

import com.simple.spark.TaskContext.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.simple.TaskContext.TaskContext

abstract class RDD[T](@transient private var _sc: SparkContext) extends Serializable with Logging {
  def compute(split: Partition, context: TaskContext): Iterator[T]
}
