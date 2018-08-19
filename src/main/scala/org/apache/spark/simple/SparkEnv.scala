package org.apache.spark.simple

import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManager
import org.apache.spark.internal.config._
import org.apache.spark.serializer.{Serializer}

class SparkEnv (
                 val executorId: String,
                 val serializer: Serializer,
                 val blockManager: BlockManager,
                 val conf: SparkConf) extends Logging {

}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  def set(e: SparkEnv) {
    env = e
  }

  /**
    * Returns the SparkEnv.
    */
  def get: SparkEnv = {
    env
  }
}