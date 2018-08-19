package org.apache.spark.simple.scheduler

import java.util.Properties

import org.apache.spark.scheduler.JobListener
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.simple.rdd.RDD
import org.apache.spark.util.CallSite

/**
  * Types of events that can be handled by the DAGScheduler. The DAGScheduler uses an event queue
  * architecture where any thread can post an event (e.g. a task finishing or a new job being
  * submitted) but there is a single "logic" thread that reads these events and takes decisions.
  * This greatly simplifies synchronization.
  */
private[scheduler] sealed trait DAGSchedulerEvent

/** A result-yielding job was submitted on a target RDD */
private[scheduler] case class JobSubmitted(
    jobId: Int,
    finalRDD: RDD[_],
    func: (TaskContext, Iterator[_]) => _,
    partitions: Array[Int],
    callSite: CallSite,
    listener: JobListener,
    properties: Properties = null)
  extends DAGSchedulerEvent
