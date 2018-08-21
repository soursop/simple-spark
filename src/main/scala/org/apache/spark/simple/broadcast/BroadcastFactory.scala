package org.apache.spark.simple.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.simple.SparkConf

import scala.reflect.ClassTag

/**
  * An interface for all the broadcast implementations in Spark (to allow
  * multiple broadcast implementations). SparkContext uses a BroadcastFactory
  * implementation to instantiate a particular broadcast for the entire Spark job.
  */
private[spark] trait BroadcastFactory {

  def initialize(isDriver: Boolean, conf: SparkConf): Unit

  /**
    * Creates a new broadcast variable.
    *
    * @param value value to broadcast
    * @param isLocal whether we are in local mode (single JVM process)
    * @param id unique id representing this broadcast variable
    */
  def newBroadcast[T: ClassTag](value: T, isLocal: Boolean, id: Long): Broadcast[T]

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean): Unit

  def stop(): Unit
}
