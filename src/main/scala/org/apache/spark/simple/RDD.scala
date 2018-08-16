package org.apache.spark.simple

import org.apache.spark.{Partition, SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.simple.TaskContext.TaskContext

abstract class RDD[T](@transient private var _sc: SparkContext) extends Serializable with Logging {
  private def sc: SparkContext = {
    if (_sc == null) {
      throw new SparkException(
        "This RDD lacks a SparkContext. It could happen in the following cases: \n(1) RDD " +
          "transformations and actions are NOT invoked by the driver, but inside of other " +
          "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid " +
          "because the values transformation and count action cannot be performed inside of the " +
          "rdd1.map transformation. For more information, see SPARK-5063.\n(2) When a Spark " +
          "Streaming job recovers from checkpoint, this exception will be hit if a reference to " +
          "an RDD not defined by the streaming job is used in DStream operations. For more " +
          "information, See SPARK-13758.")
    }
    _sc
  }

  /** A unique ID for this RDD (within its SparkContext). */
  val id: Int = sc.newRddId()

  def compute(split: Partition, context: TaskContext): Iterator[T]
}
