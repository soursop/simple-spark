package org.apache.spark.simple

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging

class SparkContext(config: SparkConf) extends Logging {

 /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world. They're mutable since we want to initialize all  |
 | of them to some neutral value ahead of time, so that calling "stop()" while the       |
 | constructor is still running is safe.                                                 |
 * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _

  try {
    _conf = config.clone()
  } catch {
    case e: Throwable =>
      logError("Error initializing SparkContext.", e)
      throw e
  }

  /* ------------------------------------------------------------------------------------- *
   | Accessors and public fields. These provide access to the internal state of the        |
   | context.                                                                              |
   * ------------------------------------------------------------------------------------- */

  private[spark] def conf: SparkConf = _conf
  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()


}
