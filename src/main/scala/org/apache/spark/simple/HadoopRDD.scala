package org.apache.spark.simple

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.simple.rdd.RDD
import org.apache.spark.util.{NextIterator, SerializableConfiguration}
import org.apache.spark.{Partition, SerializableWritable, SparkEnv}


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
                      , broadcastedConf: Broadcast[SerializableConfiguration]
                      , initLocalJobConfFuncOpt: Option[JobConf => Unit]
                      , inputFormatClass: Class[_ <: InputFormat[K, V]]
                      , keyClass: Class[K]
                      , valueClass: Class[V]
                      , minPartitions: Int
                     ) extends RDD[(K, V)](sc, Nil) {
  def this(sc: SparkContext
           , conf: SparkConf
           , initLocalJobConfFuncOpt: Option[JobConf => Unit]
           , inputFormatClass: Class[_ <: InputFormat[K, V]]
           , keyClass: Class[K]
           , valueClass: Class[V]
           , minPartitions: Int
          ) = {
    this(
      sc
      , sc.broadcast(new SerializableConfiguration(conf.asInstanceOf[Configuration]) )
      , initLocalJobConfFuncOpt
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

      var reader: RecordReader[K, V] =
        inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      override protected def getNext(): (K, V) = {
        finished = !reader.next(key, value)
        (key, value)
      }

      override protected def close(): Unit = {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              logWarning("Exception in RecordReader.close()", e)
          } finally {
            reader = null
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }

  // Returns a JobConf that will be used on slaves to obtain input splits for Hadoop reads.
  protected def getJobConf(): JobConf = {
    this.broadcastedConf.value.value match {
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



  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
    val inputSplits = allInputSplits.filter(_.getLength > 0)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }
}
