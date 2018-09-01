package org.apache.spark.simple.scheduler

import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.lang3.SerializationUtils
import org.apache.spark.internal.Logging
import org.apache.spark.simple.SparkContext
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.simple.rdd.RDD
import org.apache.spark.util.{CallSite, EventLoop, ThreadUtils}

import scala.concurrent.duration.Duration

private[spark]
class DAGScheduler(
        private[scheduler] val sc: SparkContext,
        private[scheduler] val taskScheduler: TaskSchedule)
      extends Logging {

  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)

  /**
    * Run an action job on the given RDD and pass all the results to the resultHandler function as
    * they arrive.
    *
    * @param rdd target RDD to run tasks on
    * @param func a function to run on each partition of the RDD
    * @param partitions set of partitions to run on; some jobs may not want to compute on all
    *   partitions of the target RDD, e.g. for operations like first()
    * @param callSite where in the user program this job was called
    * @param resultHandler callback to pass each result to
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    *
    * @note Throws `Exception` when the job fails
    */
  def runJob[T, U](
                    rdd: RDD[T],
                    func: (TaskContext, Iterator[T]) => U,
                    partitions: Seq[Int],
                    callSite: CallSite,
                    resultHandler: (Int, U) => Unit,
                    properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo("Job %d finished: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case scala.util.Failure(exception) =>
        logInfo("Job %d failed: %s, took %f s".format
        (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
  /**
    * Submit an action job to the scheduler.
    *
    * @param rdd target RDD to run tasks on
    * @param func a function to run on each partition of the RDD
    * @param partitions set of partitions to run on; some jobs may not want to compute on all
    *   partitions of the target RDD, e.g. for operations like first()
    * @param callSite where in the user program this job was called
    * @param resultHandler callback to pass each result to
    * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
    *
    * @return a JobWaiter object that can be used to block until the job finishes executing
    *         or can be used to cancel the job.
    *
    * @throws IllegalArgumentException when partitions ids are illegal
    */
  def submitJob[T, U](
                       rdd: RDD[T],
                       func: (TaskContext, Iterator[T]) => U,
                       partitions: Seq[Int],
                       callSite: CallSite,
                       resultHandler: (Int, U) => Unit,
                       properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {
  override protected def onReceive(event: DAGSchedulerEvent): Unit = ???

  override protected def onError(e: Throwable): Unit = ???
}
