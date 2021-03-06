package org.apache.spark.simple

import org.apache.spark.simple.rdd.RDD

/**
  * @author : soursop 
  */
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}

abstract class NarrowDependency[T](_rdd: RDD[T]) extends Dependency[T] {
  /**
    * Get the parent partitions for a child partition.
    * @param partitionId a partition of the child RDD
    * @return the partitions of the parent RDD that the child partition depends upon
    */
  def getParents(partitionId: Int): Seq[Int]

  override def rdd: RDD[T] = _rdd
}

class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = List(partitionId)
}