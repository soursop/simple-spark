package org.apache.spark.simple

import com.google.common.collect.MapMaker
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManager
import org.apache.spark.internal.config._
import org.apache.spark.serializer.Serializer
import org.apache.spark.simple.broadcast.BroadcastManager

class SparkEnv (
                 val executorId: String,
                 val serializer: Serializer,
                 val blockManager: BlockManager,
                 val broadcastManager: BroadcastManager,
                 val conf: SparkConf) extends Logging {

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()

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