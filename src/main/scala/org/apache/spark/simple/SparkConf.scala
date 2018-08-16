package org.apache.spark.simple

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf.logDeprecationWarning
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, ConfigProvider, ConfigReader, SparkConfigProvider}

import scala.collection.JavaConverters._

class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

  private val settings = new ConcurrentHashMap[String, String]()

  @transient private lazy val reader: ConfigReader = {
    val _reader = new ConfigReader(new SparkConfigProvider(settings))
    _reader.bindEnv(new ConfigProvider {
      override def get(key: String): Option[String] = Option(getenv(key))
    })
    _reader
  }

  /**
    * By using this instead of System.getenv(), environment variables can be mocked
    * in unit tests.
    */
  private[spark] def getenv(name: String): String = System.getenv(name)

  /**
    * Retrieves the value of a pre-defined configuration entry.
    *
    * - This is an internal Spark API.
    * - The return type if defined by the configuration entry.
    * - This will throw an exception is the config is not optional and the value is not set.
    */
  private[spark] def get[T](entry: ConfigEntry[T]): T = {
    entry.readFrom(reader)
  }

  private[spark] def set(key: String, value: String): SparkConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /** Copy this object */
  override def clone: SparkConf = {
    val cloned = new SparkConf(false)
    settings.entrySet().asScala.foreach {
      e => cloned.set(e.getKey(), e.getValue(), true)
    }
    cloned
  }

}
