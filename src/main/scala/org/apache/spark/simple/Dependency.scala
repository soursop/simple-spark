package org.apache.spark.simple

import org.apache.spark.simple.rdd.RDD

/**
  * @author : soursop 
  */
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
