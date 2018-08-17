package org.apache.spark.simple.deploy

import org.apache.hadoop.conf.Configuration
import org.apache.spark.simple.SparkConf
import org.apache.spark.internal.Logging

class SparkHadoopUtil extends Logging {

  /**
    * Return an appropriate (subclass) of Configuration. Creating config can initializes some Hadoop
    * subsystems.
    */
  def newConfiguration(conf: SparkConf): Configuration = {
    SparkHadoopUtil.newConfiguration(conf)
  }

}

object SparkHadoopUtil {

  private lazy val instance = new SparkHadoopUtil

  def get: SparkHadoopUtil = instance

  /**
    * Returns a Configuration object with Spark configuration applied on top. Unlike
    * the instance method, this will always return a Configuration instance, and not a
    * cluster manager-specific type.
    */
  private[spark] def newConfiguration(conf: SparkConf): Configuration = {
    val hadoopConf = new Configuration()
    appendSparkHadoopConfigs(conf, hadoopConf)
    val bufferSize = conf.get("spark.buffer.size", "65536")
    hadoopConf.set("io.file.buffer.size", bufferSize)
    hadoopConf
  }

  private def appendSparkHadoopConfigs(conf: SparkConf, hadoopConf: Configuration): Unit = {
    // Copy any "spark.hadoop.foo=bar" spark properties into conf as "foo=bar"
    for ((key, value) <- conf.getAll if key.startsWith("spark.hadoop.")) {
      hadoopConf.set(key.substring("spark.hadoop.".length), value)
    }
  }

}