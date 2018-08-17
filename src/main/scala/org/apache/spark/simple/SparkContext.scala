package org.apache.spark.simple

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.simple.rdd.{RDD, RDDOperationScope}
import org.apache.spark.simple.deploy.SparkHadoopUtil
import org.apache.spark.scheduler.TaskScheduler
import org.apache.spark.util.{ClosureCleaner, SerializableConfiguration}

class SparkContext(config: SparkConf) extends Logging {

 /* ------------------------------------------------------------------------------------- *
 | Private variables. These variables keep the internal state of the context, and are    |
 | not accessible by the outside world. They're mutable since we want to initialize all  |
 | of them to some neutral value ahead of time, so that calling "stop()" while the       |
 | constructor is still running is safe.                                                 |
 * ------------------------------------------------------------------------------------- */

  private var _conf: SparkConf = _
  private var _hadoopConfiguration: Configuration = _
  private var _taskScheduler: TaskScheduler = _

  try {
    _conf = config.clone()
    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)
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
  private[spark] def taskScheduler: TaskScheduler = _taskScheduler
  private[spark] var checkpointDir: Option[String] = None

  private val nextRddId = new AtomicInteger(0)

  /** Register a new RDD, returning its RDD ID */
  private[spark] def newRddId(): Int = nextRddId.getAndIncrement()

  /**
    * A default Hadoop Configuration for the Hadoop code (e.g. file systems) that we reuse.
    *
    * @note As it will be reused in all Hadoop RDDs, it's better not to modify it unless you
    * plan to set some global configurations for all Hadoop RDDs.
    */
  def hadoopConfiguration: Configuration = _hadoopConfiguration

  /** Default level of parallelism to use when not given by user (e.g. parallelize and makeRDD). */
  def defaultParallelism: Int = {
    taskScheduler.defaultParallelism
  }

  // Thread Local variable that can be used by users to pass information down the stack
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      SerializationUtils.clone(parent)
    }
    override protected def initialValue(): Properties = new Properties()
  }

  /**
    * Default min number of partitions for Hadoop RDDs when not given by user
    * Notice that we use math.min so the "defaultMinPartitions" cannot be higher than 2.
    * The reasons for this are discussed in https://github.com/mesos/spark/pull/718
    */
  def defaultMinPartitions: Int = math.min(defaultParallelism, 2)


  /**
    * Set a local property that affects jobs submitted from this thread, such as the Spark fair
    * scheduler pool. User-defined properties may also be set here. These properties are propagated
    * through to worker tasks and can be accessed there via
    * [[org.apache.spark.TaskContext#getLocalProperty]].
    *
    * These properties are inherited by child threads spawned from this thread. This
    * may have unexpected consequences when working with thread pools. The standard java
    * implementation of thread pools have worker threads spawn other worker threads.
    * As a result, local properties may propagate unpredictably.
    */
  def setLocalProperty(key: String, value: String) {
    if (value == null) {
      localProperties.get.remove(key)
    } else {
      localProperties.get.setProperty(key, value)
    }
  }

  /**
    * Get a local property set in this thread, or null if it is missing. See
    * `org.apache.spark.SparkContext.setLocalProperty`.
    */
  def getLocalProperty(key: String): String =
    Option(localProperties.get).map(_.getProperty(key)).orNull

  /**
    * Execute a block of code in a scope such that all new RDDs created in this body will
    * be part of the same scope. For more detail, see {{org.apache.spark.simple.rdd.RDDOperationScope}}.
    *
    * @note Return statements are NOT allowed in the given body.
    */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](this)(body)

  /**
    * Clean a closure to make it ready to be serialized and sent to tasks
    * (removes unreferenced variables in $outer's, updates REPL variables)
    * If <tt>checkSerializable</tt> is set, <tt>clean</tt> will also proactively
    * check to see if <tt>f</tt> is serializable and throw a <tt>SparkException</tt>
    * if not.
    *
    * @param f the closure to clean
    * @param checkSerializable whether or not to immediately check <tt>f</tt> for serializability
    * @throws SparkException if <tt>checkSerializable</tt> is set but <tt>f</tt> is not
    *   serializable
    * @return the cleaned closure
    */
  private[spark] def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

  /**
    * Read a text file from HDFS, a local file system (available on all nodes), or any
    * Hadoop-supported file system URI, and return it as an RDD of Strings.
    * @param path path to the text file on a supported file system
    * @param minPartitions suggested minimum number of partitions for the resulting RDD
    * @return RDD of lines of the text file
    */
  def textFile(
        path: String,
        minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *
    * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
    * operation will create many references to the same object.
    * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
    * copy them using a `map` function.
    * @param path directory to the input data files, the path can be comma separated paths
    * as a list of inputs
    * @param inputFormatClass storage format of the data to be read
    * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
    * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
    * @param minPartitions suggested minimum number of partitions for the resulting RDD
    * @return RDD of tuples of key and corresponding value
    */
  def hadoopFile[K, V](
                        path: String,
                        inputFormatClass: Class[_ <: InputFormat[K, V]],
                        keyClass: Class[K],
                        valueClass: Class[V],
                        minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = new SerializableConfiguration(hadoopConfiguration)
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

}
/**
  * The SparkContext object contains a number of implicit conversions and parameters for use with
  * various Spark features.
  */
object SparkContext extends Logging {

  private[spark] val RDD_SCOPE_KEY = "spark.rdd.scope"
  private[spark] val RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride"

}