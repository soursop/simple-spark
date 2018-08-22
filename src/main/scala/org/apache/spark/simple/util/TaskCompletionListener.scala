package org.apache.spark.simple.util

import java.util.EventListener

import org.apache.spark.simple.TaskContext.TaskContext

/**
  * :: DeveloperApi ::
  *
  * Listener providing a callback function to invoke when a task's execution completes.
  */
trait TaskCompletionListener extends EventListener {
  def onTaskCompletion(context: TaskContext): Unit
}


/**
  * :: DeveloperApi ::
  *
  * Listener providing a callback function to invoke when a task's execution encounters an error.
  * Operations defined here must be idempotent, as `onTaskFailure` can be called multiple times.
  */
trait TaskFailureListener extends EventListener {
  def onTaskFailure(context: TaskContext, error: Throwable): Unit
}


/**
  * Exception thrown when there is an exception in executing the callback in TaskCompletionListener.
  */
private[spark]
class TaskCompletionListenerException(
                                       errorMessages: Seq[String],
                                       val previousError: Option[Throwable] = None)
  extends RuntimeException {

  override def getMessage: String = {
    val listenerErrorMessage =
      if (errorMessages.size == 1) {
        errorMessages.head
      } else {
        errorMessages.zipWithIndex.map { case (msg, i) => s"Exception $i: $msg" }.mkString("\n")
      }
    val previousErrorMessage = previousError.map { e =>
      "\n\nPrevious exception in task: " + e.getMessage + "\n" +
        e.getStackTrace.mkString("\t", "\n\t", "")
    }.getOrElse("")
    listenerErrorMessage + previousErrorMessage
  }
}
