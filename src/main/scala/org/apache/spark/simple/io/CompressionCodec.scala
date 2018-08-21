package org.apache.spark.simple.io

import java.io.{InputStream, OutputStream}
import java.util.Locale

import org.apache.spark.io._
import org.apache.spark.simple.SparkConf
import org.apache.spark.simple.util.Utils

trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}

private[spark] object CompressionCodec {

  private val configKey = "spark.io.compression.codec"

  private[spark] def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean = {
    (codec.isInstanceOf[SnappyCompressionCodec] || codec.isInstanceOf[LZFCompressionCodec]
      || codec.isInstanceOf[LZ4CompressionCodec] || codec.isInstanceOf[ZStdCompressionCodec])
  }

  private val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName)

  def getCodecName(conf: SparkConf): String = {
    conf.get(configKey, DEFAULT_COMPRESSION_CODEC)
  }

  def createCodec(conf: SparkConf): CompressionCodec = {
    createCodec(conf, getCodecName(conf))
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val codecClass =
      shortCompressionCodecNames.getOrElse(codecName.toLowerCase(Locale.ROOT), codecName)
    val codec = try {
      val ctor = Utils.classForName(codecClass).getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
    } catch {
      case _: ClassNotFoundException | _: IllegalArgumentException => None
    }
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available. " +
      s"Consider setting $configKey=$FALLBACK_COMPRESSION_CODEC"))
  }

  /**
    * Return the short version of the given codec name.
    * If it is already a short name, just return it.
    */
  def getShortName(codecName: String): String = {
    if (shortCompressionCodecNames.contains(codecName)) {
      codecName
    } else {
      shortCompressionCodecNames
        .collectFirst { case (k, v) if v == codecName => k }
        .getOrElse { throw new IllegalArgumentException(s"No short name for codec $codecName.") }
    }
  }

  val FALLBACK_COMPRESSION_CODEC = "snappy"
  val DEFAULT_COMPRESSION_CODEC = "lz4"
  val ALL_COMPRESSION_CODECS = shortCompressionCodecNames.values.toSeq
}