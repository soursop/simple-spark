package com.simple.spark


object TaskContext {
  /**
    * Return the currently active TaskContext. This can be called inside of
    * user functions to access contextual information about running tasks.
    */
  def get(): TaskContext = taskContext.get

  /**
    * Returns the partition id of currently active TaskContext. It will return 0
    * if there is no active TaskContext for cases like local execution.
    */
  def getPartitionId(): Int = {
    val tc = taskContext.get()
    if (tc eq null) {
      0
    } else {
      tc.partitionId()
    }
  }

  private[this] val taskContext: ThreadLocal[TaskContext] = new ThreadLocal[TaskContext]

  abstract class TaskContext extends Serializable {

    /**
      * Returns true if the task has completed.
      */
    def isCompleted(): Boolean
    /**
      * The ID of the RDD partition that is computed by this task.
      */
    def partitionId(): Int
  }
}
