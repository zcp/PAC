/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.io

import java.io._
import java.io.{BufferedReader, BufferedWriter, ByteArrayOutputStream, File, FileInputStream, FileWriter, IOException, InputStreamReader, OutputStream}
import java.util.Locale
import java.util.zip.{Deflater, DeflaterOutputStream, InflaterInputStream}
import com.github.luben.zstd.{ZstdInputStream, ZstdOutputStream}
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream, LZ4Factory}
import net.jpountz.xxhash.XXHashFactory
import org.xerial.snappy.{Snappy, SnappyInputStream, SnappyOutputStream}
import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.config._
//import org.apache.spark.io.{ZlibInputStream, ZlibOutputStream}
import org.apache.spark.util.Utils
import org.slf4j.LoggerFactory

import java.util.logging.Logger
import scala.io.Source



/**
 * :: DeveloperApi ::
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 *
 * @note The wire protocol for a codec is not guaranteed compatible across versions of Spark.
 * This is intended for use as an internal compression utility within a single Spark application.
 */
@DeveloperApi
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  private[spark] def compressedContinuousOutputStream(s: OutputStream): OutputStream = {
    compressedOutputStream(s)
  }

  def compressedInputStream(s: InputStream): InputStream

  private[spark] def compressedContinuousInputStream(s: InputStream): InputStream = {
    compressedInputStream(s)
  }
}

private[spark] object CompressionCodec {

  private val configKey = IO_COMPRESSION_CODEC.key

  private[spark] def supportsConcatenationOfSerializedStreams(codec: CompressionCodec): Boolean = {
    (codec.isInstanceOf[SnappyCompressionCodec] || codec.isInstanceOf[LZFCompressionCodec]
      || codec.isInstanceOf[LZ4CompressionCodec] || codec.isInstanceOf[ZStdCompressionCodec]
      || codec.isInstanceOf[KAEzipCompressionCodec])
  }

  private val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName,
    "zstd" -> classOf[ZStdCompressionCodec].getName,
    // cqut,the following two lines are used to support for zlib and kaezip
    "zlib" -> classOf[ZlibCompressionCodec].getName,
    "kaezip" -> classOf[KAEzipCompressionCodec].getName)

  def getCodecName(conf: SparkConf): String = {
    // val kaezip_buffer = conf.get("KAEZIP_BUFFERSIZE")
    conf.get(IO_COMPRESSION_CODEC)

  }

  def createCodec(conf: SparkConf): CompressionCodec = {
    createCodec(conf, getCodecName(conf))
  }

  def createCodec(conf: SparkConf, codecName: String): CompressionCodec = {
    val logger = LoggerFactory.getLogger(this.getClass)
    logger.info("createCodec,codecName," + codecName)
    // logger.error("*")
    val codecClass =
      shortCompressionCodecNames.getOrElse(codecName.toLowerCase(Locale.ROOT), codecName)
    logger.info("createCodec,codecClass," + codecClass)
    val codec = try {
      val ctor =
        Utils.classForName[CompressionCodec](codecClass).getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(conf))
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

/**
 * :: DeveloperApi ::
 * LZ4 implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.lz4.blockSize`.
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class LZ4CompressionCodec(conf: SparkConf) extends CompressionCodec {

  // SPARK-28102: if the LZ4 JNI libraries fail to initialize then `fastestInstance()` calls fall
  // back to non-JNI implementations but do not remember the fact that JNI failed to load, so
  // repeated calls to `fastestInstance()` will cause performance problems because the JNI load
  // will be repeatedly re-attempted and that path is slow because it throws exceptions from a
  // static synchronized method (causing lock contention). To avoid this problem, we cache the
  // result of the `fastestInstance()` calls ourselves (both factories are thread-safe).
  @transient private[this] lazy val lz4Factory: LZ4Factory = LZ4Factory.fastestInstance()
  @transient private[this] lazy val xxHashFactory: XXHashFactory = XXHashFactory.fastestInstance()

  private[this] val defaultSeed: Int = 0x9747b28c // LZ4BlockOutputStream.DEFAULT_SEED

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.get(IO_COMPRESSION_LZ4_BLOCKSIZE).toInt
    val syncFlush = false
    new LZ4BlockOutputStream(
      s,
      blockSize,
      lz4Factory.fastCompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      syncFlush)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    val disableConcatenationOfByteStream = false
    new LZ4BlockInputStream(
      s,
      lz4Factory.fastDecompressor(),
      xxHashFactory.newStreamingHash32(defaultSeed).asChecksum,
      disableConcatenationOfByteStream)
  }
}


/**
 * :: DeveloperApi ::
 * LZF implementation of [[org.apache.spark.io.CompressionCodec]].
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class LZFCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}


/**
 * :: DeveloperApi ::
 * Snappy implementation of [[org.apache.spark.io.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.snappy.blockSize`.
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class SnappyCompressionCodec(conf: SparkConf) extends CompressionCodec {

  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException(e)
  }

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.get(IO_COMPRESSION_SNAPPY_BLOCKSIZE).toInt
    new SnappyOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}

/**
 * :: DeveloperApi ::
 * ZStandard implementation of [[org.apache.spark.io.CompressionCodec]]. For more
 * details see - http://facebook.github.io/zstd/
 *
 * @note The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
@DeveloperApi
class ZStdCompressionCodec(conf: SparkConf) extends CompressionCodec {

  private val bufferSize = conf.get(IO_COMPRESSION_ZSTD_BUFFERSIZE).toInt
  // Default compression level for zstd compression to 1 because it is
  // fastest of all with reasonably high compression ratio.
  private val level = conf.get(IO_COMPRESSION_ZSTD_LEVEL)

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    // Wrap the zstd output stream in a buffered output stream, so that we can
    // avoid overhead excessive of JNI call while trying to compress small amount of data.
    new BufferedOutputStream(new ZstdOutputStream(s, level), bufferSize)
  }

  override private[spark] def compressedContinuousOutputStream(s: OutputStream) = {
    // SPARK-29322: Set "closeFrameOnFlush" to 'true' to let continuous input stream not being
    // stuck on reading open frame.
    new BufferedOutputStream(new ZstdOutputStream(s, level).setCloseFrameOnFlush(true), bufferSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    // Wrap the zstd input stream in a buffered input stream so that we can
    // avoid overhead excessive of JNI call while trying to uncompress small amount of data.
    new BufferedInputStream(new ZstdInputStream(s), bufferSize)
  }

  override def compressedContinuousInputStream(s: InputStream): InputStream = {
    // SPARK-26283: Enable reading from open frames of zstd (for eg: zstd compressed eventLog
    // Reading). By default `isContinuous` is false, and when we try to read from open frames,
    // `compressedInputStream` method above throws truncated error exception. This method set
    // `isContinuous` true to allow reading from open frames.
    new BufferedInputStream(new ZstdInputStream(s).setContinuous(true), bufferSize)
  }
}

//cqut,the class is used to support for zlib
@DeveloperApi
class ZlibCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    // Wrap the zstd output stream in a buffered output stream, so that we can
    // avoid overhead excessive of JNI call while trying to compress small amount of data.
    // val logger = LoggerFactory.getLogger(this.getClass)
    // logger.info("ZlibOutputStream will be used.")
    // print("compressedOutputStream is called\n")
    val buff_size = conf.get("spark.zlib.buffersize").toInt * 1024
    val shufflewrite_opti = conf.get("spark.shufflewrite.optimization")
    val logger_flag = conf.get("spark.mylogger.flag")
    val logger = LoggerFactory.getLogger(this.getClass)
    //System.out.println("zcp,zcp," + shufflewrite_opti + "," + logger_flag);
    var out: OutputStream = null;
    val stream_id = System.nanoTime()

    if (shufflewrite_opti.equals("t")) {
      if (logger_flag.equals("t")) {
        logger.info(s"ZlibOutputStream2 is initialized, buff_size," +
          s"${buff_size}, stream_id, ${stream_id}")
      }
      // logger.info("KaezipOutputStream2 is initialized")
      out = new ZlibOutputStream2(s, buff_size, stream_id)
    } else {
      if (logger_flag.equals("t")) {
        logger.info("ZlibOutputStream is initialized")
      }
      out = new ZlibOutputStream(s, buff_size)
    }

    out
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    // Wrap the zstd input stream in a buffered input stream so that we can
    // avoid overhead excessive of JNI call while trying to uncompress small amount of data.
    // val logger = LoggerFactory.getLogger(this.getClass)
    // logger.info("ZlibInputStream will be used.")

    val buff_size = conf.get("spark.zlib.buffersize").toInt * 1024
    val shufflewrite_opti = conf.get("spark.shuffleread.optimization")
    val logger_flag = conf.get("spark.mylogger.flag")
    val logger = LoggerFactory.getLogger(this.getClass)

    var in: InputStream = null;
    val stream_id = System.nanoTime()

    if (shufflewrite_opti.equals("t")) {
      if (logger_flag.equals("t")) {
        logger.info(s"ZlibInputStream2 is initialized, buff_size," +
          s"${buff_size}, stream_id, ${stream_id}")
      }
      // logger.info("KaezipOutputStream2 is initialized")
      in = new ZlibInputStream2(s, buff_size, stream_id)
    } else {
      if (logger_flag.equals("t")) {
        logger.info("ZlibInputStream is initialized")
      }
      in = new ZlibInputStream(s, buff_size)
    }
    in
  }
}
//cqut,the class is used to support for kaezip
@DeveloperApi
class KAEzipCompressionCodec(conf: SparkConf) extends CompressionCodec {
  private var buff_size = 0;
  private var output_optimization = ""
  private var input_optimization = ""


  override def compressedOutputStream(s: OutputStream): OutputStream = {
    // Wrap the zstd output stream in a buffered output stream, so that we can
    // avoid overhead excessive of JNI call while trying to compress small amount of data.
    // val logger = LoggerFactory.getLogger(this.getClass)
    // logger.info("KaezipOutputStream will be used.")

    val buff_size = conf.get("spark.kaezip.buffersize").toInt * 1024
    val shufflewrite_opti = conf.get("spark.shufflewrite.optimization")
    val logger_flag = conf.get("spark.mylogger.flag")

    val logger = LoggerFactory.getLogger(this.getClass)

    var out: OutputStream = null;
    val stream_id = System.nanoTime()

    if (shufflewrite_opti.equals("t")) {
      if (logger_flag.equals("t")) {
        logger.info(s"KaezipOutputStream2 is initialized, buff_size," +
                    s"${buff_size}, stream_id, ${stream_id}")
      }
        // logger.info("KaezipOutputStream2 is initialized")
        out = new KaezipOutputStream2(s, buff_size, stream_id)
    } else {
      if (logger_flag.equals("t")) {
        logger.info("KaezipOutputStream is initialized")
      }
        out = new KaezipOutputStream(s, buff_size)
    }

    val exec_time = System.nanoTime() - stream_id
    if (logger_flag.equals("t")) {
      val file = new File("/home/compression_tracer/compressedOutputStream-" + stream_id)
      val pw = new PrintWriter(file)
      val content = s"compressedOutputStream is created, stream_id, execution time(ns), " +
                    s"${stream_id}, ${exec_time}"
      pw.write(content + "\n")
      pw.close()
    }

    out
  }

  override def compressedInputStream(s: InputStream): InputStream = {
    // Wrap the zstd input stream in a buffered input stream so that we can
    // avoid overhead excessive of JNI call while trying to uncompress small amount of data.
    // val logger = LoggerFactory.getLogger(this.getClass)
    // logger.info("KaezipInputStream will be used.")
    val buff_size = conf.get("spark.kaezip.buffersize").toInt * 1024
    val shuffleread_opti = conf.get("spark.shuffleread.optimization")
    val logger_flag = conf.get("spark.mylogger.flag")
    val logger = LoggerFactory.getLogger(this.getClass)

    var in: InputStream = null
    val stream_id = System.nanoTime()

    if (shuffleread_opti.equals("t")) {
      if (logger_flag.equals("t")) {
         logger.info(s"KaezipInputStream2 is initialized, buff_size,${buff_size}, " +
                     s"stream_id, ${stream_id}")
      }
      in = new KaezipInputStream2(s, buff_size, stream_id)
    }
    else {
      if (logger_flag.equals("t")) {
        logger.info("KaezipInputStream is initialized")
      }
      in = new KaezipInputStream(s, buff_size)
    }

    val exec_time = System.nanoTime() - stream_id
    if (logger_flag.equals("t")) {
      val file = new File("/home/compression_tracer/compressedInputStream-" + stream_id)
      val pw = new PrintWriter(file)
      val content = s"compressedInputStream is created, stream_id, execution time(ns), " +
                    s"${stream_id}, ${exec_time}"
      pw.write(content + "\n")
      pw.close()
    }
    in
  }

}