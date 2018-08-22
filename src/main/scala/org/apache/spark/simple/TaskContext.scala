package org.apache.spark.simple

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
    /**
      * If the task is interrupted, throws TaskKilledException with the reason for the interrupt.
      */
    private[spark] def killTaskIfInterrupted(): Unit
    /**
      * How many times this task has been attempted.  The first task attempt will be assigned
      * attemptNumber = 0, and subsequent attempts will have increasing attempt numbers.
      */
    def attemptNumber(): Int
    /**
      * Adds a listener in the form of a Scala closure to be executed on task completion.
      * This will be called in all situations - success, failure, or cancellation. Adding a listener
      * to an already completed task will result in that listener being called immediately.
      *
      * An example use is for HadoopRDD to register a callback to close the input stream.
      *
      * Exceptions thrown by the listener will result in failure of the task.
      */
    def addTaskCompletionListener(f: (TaskContext) => Unit): TaskContext = {
      addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = f(context)
      })
    }
  }
}
