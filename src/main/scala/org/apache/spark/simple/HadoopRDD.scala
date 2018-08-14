package org.apache.spark.simple

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.hadoop.mapred.{FileSplit, InputSplit, RecordReader, Reporter}
import org.apache.spark.SerializableWritable
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, InputFileBlockHolder}
import org.apache.spark.simple.TaskContext.TaskContext
import org.apache.spark.util.NextIterator


/**
  * A Spark split class that wraps around a Hadoop InputSplit.
  */
private[spark] class HadoopPartition(rddId: Int, override val index: Int, s: InputSplit)
  extends Partition {

  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

class HadoopRDD[K, V](sc: SparkContext) extends RDD[(K, V)](sc) {
  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {

    val iter = new NextIterator[(K, V)] {
      private val split = theSplit.asInstanceOf[HadoopPartition]

      override protected def getNext(): (K, V) = ???

      override protected def close(): Unit = ???
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }
}
