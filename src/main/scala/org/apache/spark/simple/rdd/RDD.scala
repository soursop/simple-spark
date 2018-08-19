package org.apache.spark.simple.rdd

import org.apache.spark.internal.Logging
import org.apache.spark.simple.{Dependency, SparkContext}
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, Partitioner, SparkException}

import scala.reflect.ClassTag

abstract class RDD[T](@transient private var _sc: SparkContext
                     , @transient private var deps: Seq[Dependency[_]]
                     ) extends Serializable with Logging {
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

  /** A friendly name for this RDD */
  @transient var name: String = _

  // Our dependencies and partitions will be gotten by calling subclass's methods below, and will
  // be overwritten when we're checkpointed
  private var dependencies_ : Seq[Dependency[_]] = _

  @transient private var partitions_ : Array[Partition] = _

  // Avoid handling doCheckpoint multiple times to prevent excessive recursion
  @transient private var doCheckpointCalled = false

  private[spark] var checkpointData: Option[RDDCheckpointData[T]] = None

  private[spark] def conf = sc.conf

  /** The [[org.apache.spark.SparkContext]] that this RDD was created on. */
  def context: SparkContext = sc

  /** The SparkContext that created this RDD. */
  def sparkContext: SparkContext = sc

  // =======================================================================
  // Other internal methods and fields
  // =======================================================================

  private var storageLevel: StorageLevel = StorageLevel.NONE

  /** Get the RDD's current storage level, or StorageLevel.NONE if none is set. */
  def getStorageLevel: StorageLevel = storageLevel

  /** Assign a name to this RDD */
  def setName(_name: String): this.type = {
    name = _name
    this
  }

  /**
    * Return whether this RDD is marked for local checkpointing.
    * Exposed for testing.
    */
  private[rdd] def isLocallyCheckpointed: Boolean = {
    checkpointData match {
      case Some(_: LocalRDDCheckpointData[T]) => true
      case _ => false
    }
  }

  /**
    * Return whether this RDD is checkpointed and materialized, either reliably or locally.
    * This is introduced as an alias for `isCheckpointed` to clarify the semantics of the
    * return value. Exposed for testing.
    */
  private[spark] def isCheckpointedAndMaterialized: Boolean =
    checkpointData.exists(_.isCheckpointed)

  /**
    * Execute a block of code in a scope such that all new RDDs created in this body will
    * be part of the same scope. For more detail, see {{org.apache.spark.rdd.RDDOperationScope}}.
    *
    * Note: Return statements are NOT allowed in the given body.
    */
  private[spark] def withScope[U](body: => U): U = RDDOperationScope.withScope[U](sc)(body)

  /**
    * Clears the dependencies of this RDD. This method must ensure that all references
    * to the original parent RDDs are removed to enable the parent RDDs to be garbage
    * collected. Subclasses of RDD may override this method for implementing their own cleaning
    * logic. See [[org.apache.spark.rdd.UnionRDD]] for an example.
    */
  protected def clearDependencies(): Unit = {
    dependencies_ = null
  }

  /**
    * Optionally overridden by subclasses to specify placement preferences.
    */
  protected def getPreferredLocations(split: Partition): Seq[String] = Nil
  /**
    * Gets the name of the directory to which this RDD was checkpointed.
    * This is not defined if the RDD is checkpointed locally.
    */
  def getCheckpointFile: Option[String] = {
    checkpointData match {
      case Some(reliable: ReliableRDDCheckpointData[T]) => reliable.getCheckpointDir
      case _ => None
    }
  }

  /** Optionally overridden by subclasses to specify how they are partitioned. */
  @transient val partitioner: Option[Partitioner] = None

  /** An Option holding our checkpoint RDD, if we are checkpointed */
  private def checkpointRDD: Option[CheckpointRDD[T]] = checkpointData.flatMap(_.checkpointRDD)

  /** Construct an RDD with just a one-to-one dependency on one parent */
  def this(@transient oneParent: RDD[_]) =
    this(oneParent.context, List(new OneToOneDependency(oneParent)))

  /**
    * Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    */
  protected def getDependencies: Seq[Dependency[_]] = deps

  /**
    * Get the list of dependencies of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    */
  final def dependencies: Seq[Dependency[_]] = {
    checkpointRDD.map(r => List(new OneToOneDependency(r))).getOrElse {
      if (dependencies_ == null) {
        dependencies_ = getDependencies
      }
      dependencies_
    }
  }

  /** Returns the first parent RDD */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /**
    * Performs the checkpointing of this RDD by saving this. It is called after a job using this RDD
    * has completed (therefore the RDD has been materialized and potentially stored in memory).
    * doCheckpoint() is called recursively on the parent RDDs.
    */
  private[spark] def doCheckpoint(): Unit = {
    RDDOperationScope.withScope(sc, "checkpoint", allowNesting = false, ignoreParent = true) {
      if (!doCheckpointCalled) {
        doCheckpointCalled = true
        checkpointData match {
          case Some(data) => data.checkpoint()
          case None => dependencies.foreach(_.rdd.doCheckpoint())
        }
      }
    }
  }

  /**
    * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
    * directory set with `SparkContext#setCheckpointDir` and all references to its parent
    * RDDs will be removed. This function must be called before any job has been
    * executed on this RDD. It is strongly recommended that this RDD is persisted in
    * memory, otherwise saving it on a file will require recomputation.
    */
  def checkpoint(): Unit = RDDCheckpointData.synchronized {
    // NOTE: we use a global lock here due to complexities downstream with ensuring
    // children RDD partitions point to the correct parent partitions. In the future
    // we should revisit this consideration.
    if (context.checkpointDir.isEmpty) {
      throw new SparkException("Checkpoint directory has not been set in the SparkContext")
    } else if (checkpointData.isEmpty) {
      checkpointData = Some(new ReliableRDDCheckpointData(this))
    }
  }
  /**
    * Mark this RDD for local checkpointing using Spark's existing caching layer.
    *
    * This method is for users who wish to truncate RDD lineages while skipping the expensive
    * step of replicating the materialized data in a reliable distributed file system. This is
    * useful for RDDs with long lineages that need to be truncated periodically (e.g. GraphX).
    *
    * Local checkpointing sacrifices fault-tolerance for performance. In particular, checkpointed
    * data is written to ephemeral local storage in the executors instead of to a reliable,
    * fault-tolerant storage. The effect is that if an executor fails during the computation,
    * the checkpointed data may no longer be accessible, causing an irrecoverable job failure.
    *
    * This is NOT safe to use with dynamic allocation, which removes executors along
    * with their cached blocks. If you must use both features, you are advised to set
    * `spark.dynamicAllocation.cachedExecutorIdleTimeout` to a high value.
    *
    * The checkpoint directory set through `SparkContext#setCheckpointDir` is not used.
    */
  def localCheckpoint(): this.type = RDDCheckpointData.synchronized {

    // Note: At this point we do not actually know whether the user will call persist() on
    // this RDD later, so we must explicitly call it here ourselves to ensure the cached
    // blocks are registered for cleanup later in the SparkContext.
    //
    // If, however, the user has already called persist() on this RDD, then we must adapt
    // the storage level he/she specified to one that is appropriate for local checkpointing
    // (i.e. uses disk) to guarantee correctness.

    if (storageLevel == StorageLevel.NONE) {
      persist(LocalRDDCheckpointData.DEFAULT_STORAGE_LEVEL)
    } else {
      persist(LocalRDDCheckpointData.transformStorageLevel(storageLevel), allowOverride = true)
    }

    // If this RDD is already checkpointed and materialized, its lineage is already truncated.
    // We must not override our `checkpointData` in this case because it is needed to recover
    // the checkpointed data. If it is overridden, next time materializing on this RDD will
    // cause error.
    if (isCheckpointedAndMaterialized) {
      logWarning("Not marking RDD for local checkpoint because it was already " +
        "checkpointed and materialized")
    } else {
      // Lineage is not truncated yet, so just override any existing checkpoint data with ours
      checkpointData match {
        case Some(_: ReliableRDDCheckpointData[_]) => logWarning(
          "RDD was already marked for reliable checkpointing: overriding with local checkpoint.")
        case _ =>
      }
      checkpointData = Some(new LocalRDDCheckpointData(this))
    }
    this
  }

  /**
    * Set this RDD's storage level to persist its values across operations after the first time
    * it is computed. This can only be used to assign a new storage level if the RDD does not
    * have a storage level set yet. Local checkpointing is an exception.
    */
  def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {
      // This means the user previously called localCheckpoint(), which should have already
      // marked this RDD for persisting. Here we should override the old storage level with
      // one that is explicitly requested by the user (after adapting it to use disk).
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * The partitions in this array must satisfy the following property:
    *   `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    */
  protected def getPartitions: Array[Partition]

  /**
    * Get the array of partitions of this RDD, taking into account whether the
    * RDD is checkpointed or not.
    */
  final def partitions: Array[Partition] = {
    checkpointRDD.map(_.partitions).getOrElse {
      if (partitions_ == null) {
        partitions_ = getPartitions
        partitions_.zipWithIndex.foreach { case (partition, index) =>
          require(partition.index == index,
            s"partitions($index).partition == ${partition.index}, but it should equal $index")
        }
      }
      partitions_
    }
  }

  /**
    * Changes the dependencies of this RDD from its original parents to a new RDD (`newRDD`)
    * created from the checkpoint file, and forget its old dependencies and partitions.
    */
  private[spark] def markCheckpointed(): Unit = {
    clearDependencies()
    partitions_ = null
    deps = null    // Forget the constructor argument for dependencies too
  }

  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
    * Return a new RDD by applying a function to all elements of this RDD.
    */
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
}
