package org.apache.spark.simple

trait Partition extends Serializable  {
  /**
    * Get the partition's index within its parent RDD
    */
  def index: Int

  // A better default implementation of HashCode
  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)

}
