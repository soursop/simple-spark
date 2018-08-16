package org.apache.spark.simple

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.spark.{Partition, SerializableWritable, SparkEnv}
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.config.IGNORE_CORRUPT_FILES
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, InputFileBlockHolder}
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.util.{NextIterator, SerializableConfiguration}


/**
  * A Spark split class that wraps around a Hadoop InputSplit.
  */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

class HadoopRDD[K, V](sc: SparkContext
                      , conf: SerializableConfiguration
                      , inputFormatClass: Class[_ <: InputFormat[K, V]]
                      , keyClass: Class[K]
                      , valueClass: Class[V]
                      , minPartitions: Int
                     ) extends RDD[(K, V)](sc) {
    def this(sc: SparkContext
      , conf: SparkConf
      , inputFormatClass: Class[_ <: InputFormat[K, V]]
      , keyClass: Class[K]
      , valueClass: Class[V]
      , minPartitions: Int
    ) = {
      this(
        sc
        , new SerializableConfiguration(conf.asInstanceOf[Configuration])
        , inputFormatClass
        , keyClass
        , valueClass
        , minPartitions
      )
    }

  /**
    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
    * Therefore, we synchronize on this lock before calling new JobConf() or new Configuration().
    */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  protected val jobConfCacheKey: String = "rdd_%d_job_conf".format(id)


  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {

    val iter = new NextIterator[(K, V)] {
      private val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)

      val jobConf = getJobConf()

      private val inputFormat = getInputFormat(jobConf)

      val reader: RecordReader[K, V] =
      inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override protected def getNext(): (K, V) = {
        finished = !reader.next(key, value)
//        if (!finished) {
//          inputMetrics.incRecordsRead(1)
//        }
//        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
//          updateBytesRead()
//        }
        (key, value)
      }

      override protected def close(): Unit = ???
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    this.conf.value match {
      case conf: JobConf =>
        logDebug("Re-using user-broadcasted JobConf")
        conf.asInstanceOf[JobConf]
      case conf =>
        Option(getCachedMetadata(jobConfCacheKey))
          .map { conf =>
            logDebug("Re-using cached JobConf")
            conf.asInstanceOf[JobConf]
          }
          .getOrElse {
            // Create a JobConf that will be cached and used across this RDD's getJobConf() calls in
            // the local process. The local cache is accessed through HadoopRDD.putCachedMetadata().
            // The caching helps minimize GC, since a JobConf can contain ~10KB of temporary
            // objects. Synchronize to prevent ConcurrentModificationException (SPARK-1097,
            // HADOOP-10456).
            CONFIGURATION_INSTANTIATION_LOCK.synchronized {
              logDebug("Creating new JobConf and caching it for later re-use")
              val newJobConf = new JobConf(conf)
              putCachedMetadata(jobConfCacheKey, newJobConf)
              newJobConf
            }
          }
    }
  }

  /**
    * The three methods below are helpers for accessing the local map, a property of the SparkEnv of
    * the local process.
    */
  def getCachedMetadata(key: String): Any = SparkEnv.get.hadoopJobMetadata.get(key)

  private def putCachedMetadata(key: String, value: Any): Unit =
    SparkEnv.get.hadoopJobMetadata.put(key, value)

  protected def getInputFormat(conf: JobConf): InputFormat[K, V] = {
    ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
  }

}
