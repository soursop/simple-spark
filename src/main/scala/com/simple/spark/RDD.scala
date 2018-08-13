package com.simple.spark

trait RDD[T] {
  def compute(): T
}
