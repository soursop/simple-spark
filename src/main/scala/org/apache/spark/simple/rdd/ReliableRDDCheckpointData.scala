package org.apache.spark.simple.rdd

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.simple.SparkContext

import scala.reflect.ClassTag

/**
  * An implementation of checkpointing that writes the RDD data to reliable storage.
  * This allows drivers to be restarted on failure with previously computed state.
  */
private[spark] class ReliableRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  // The directory to which the associated RDD has been checkpointed to
  // This is assumed to be a non-local path that points to some reliable storage
  private val cpDir: String =
  ReliableRDDCheckpointData.checkpointPath(rdd.context, rdd.id)
    .map(_.toString)
    .getOrElse { throw new SparkException("Checkpoint dir must be specified.") }

  /**
    * Return the directory to which this RDD was checkpointed.
    * If the RDD is not checkpointed yet, return None.
    */
  def getCheckpointDir: Option[String] = RDDCheckpointData.synchronized {
    if (isCheckpointed) {
      Some(cpDir.toString)
    } else {
      None
    }
  }

  /**
    * Materialize this RDD and write its content to a reliable DFS.
    * This is called immediately after the first action invoked on this RDD has completed.
    */
  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val newRDD = ReliableCheckpointRDD.writeRDDToCheckpointDirectory(rdd, cpDir)

    logInfo(s"Done checkpointing RDD ${rdd.id} to $cpDir, new parent is RDD ${newRDD.id}")
    newRDD
  }

}

private[spark] object ReliableRDDCheckpointData extends Logging {

  /** Return the path of the directory to which this RDD's checkpoint data is written. */
  def checkpointPath(sc: SparkContext, rddId: Int): Option[Path] = {
    sc.checkpointDir.map { dir => new Path(dir, s"rdd-$rddId") }
  }

  /** Clean up the files associated with the checkpoint data for this RDD. */
  def cleanCheckpoint(sc: SparkContext, rddId: Int): Unit = {
    checkpointPath(sc, rddId).foreach { path =>
      path.getFileSystem(sc.hadoopConfiguration).delete(path, true)
    }
  }
}