package org.apache.spark.simple.scheduler

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.JobListener

import scala.concurrent.{Future, Promise}

/**
  * An object that waits for a DAGScheduler job to complete. As tasks finish, it passes their
  * results to the given handler function.
  */
private[spark] class JobWaiter[T](
                                   dagScheduler: DAGScheduler,
                                   val jobId: Int,
                                   totalTasks: Int,
                                   resultHandler: (Int, T) => Unit)
  extends JobListener with Logging {

  private val finishedTasks = new AtomicInteger(0)
  // If the job is finished, this will be its result. In the case of 0 task jobs (e.g. zero
  // partition RDDs), we set the jobResult directly to JobSucceeded.
  private val jobPromise: Promise[Unit] =
  if (totalTasks == 0) Promise.successful(()) else Promise()

  def jobFinished: Boolean = jobPromise.isCompleted

  def completionFuture: Future[Unit] = jobPromise.future

  override def taskSucceeded(index: Int, result: Any): Unit = ???

  override def jobFailed(exception: Exception): Unit = ???
}
