package org.apache.spark.simple

import TaskContext.TaskContext

class InterruptibleIterator[+T](val context: TaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  def hasNext: Boolean = {
    // TODO(aarondav/rxin): Check Thread.interrupted instead of context.interrupted if interrupt
    // is allowed. The assumption is that Thread.interrupted does not have a memory fence in read
    // (just a volatile field in C), while context.interrupted is a volatile in the JVM, which
    // introduces an expensive read fence.
    context.killTaskIfInterrupted()
    delegate.hasNext
  }

  def next(): T = delegate.next()
}
