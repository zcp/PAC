
package org.apache.spark.io

import com.github.luben.zstd.ZstdOutputStream

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import scala.collection.mutable.ListBuffer

object ResolveShuffleOutput{
    def readOrigFile_RF(input: String): Unit = {
      val fs: FileInputStream = new FileInputStream(input)
      var in: ObjectInputStream = new ObjectInputStream(fs);
      var count = 0;
      while (fs.available() > 0) {
        try {
          if (count %10 == 0 && count != 0) {
            in = new ObjectInputStream(fs)
          }
          val obj = in.readObject()
          count = count + 1
          System.out.print("obj #" + count + " is a: " + obj.getClass() + "\n")
          System.out.print(obj + ".toString(): " + obj.toString + "\n");
        } catch {
          case ex: java.io.StreamCorruptedException => print(ex + "\n")
        }
      }

      fs.close()
      in.close()
    }

    def readOrigFile(input: String): Unit = {
      val fs: FileInputStream = new FileInputStream(input)
      val in = new ObjectInputStream(fs);
      var count = 0;
      try {
        while (true) {
          count = count + 1
            val obj = in.readObject()
            System.out.print("obj #" + count + " is a: " + obj.getClass() + "\n")
            System.out.print(obj + ".toString(): " + obj.toString + "\n");
        }
      } catch {
        case ex: java.io.EOFException => print("found a unknown exception," + ex + "\n")
      }

      fs.close()
      in.close()
    }

    def uncompress(input: String): Unit = {
      // val filePath = "/home/zcp/kunpeng/kaezip_lda/13589327364703_temp_shuffle_fb9bcfc2-07ed-43d8-8b61-dc835fad3ba1"
      val filePath2 = "/home/zcp/kunpeng/kaezip_lda/1358_ser.orig"
      val filePath3 = "/home/zcp/kunpeng/kaezip_lda/3371393487424160_temp_shuffle_1c358a9e-9530-4cf4-8d6d-f9da4bea070b.zlib"

      val kae_in = new KaeInflaterInputStream(new FileInputStream(input));
      val in_obj = new ObjectInputStream(kae_in)

      try {
        var count = 0
        while (true) {
          val obj = in_obj.readObject()
          count = count + 1
          print("obj #" + count + " is a: " + obj.getClass() + "\n")
          print(obj + ".toString(): " + obj.toString + "\n")
        }
        // in.close()
      }
      catch {
        case ex: Throwable => print("found a unknown exception," + ex + "\n")
      }
    }

    def _compress_rf(input: String, out: OutputStream): Long = {
      val start_time = System.nanoTime()
      val out_obj = new ObjectOutputStream(out)

      val fs: FileInputStream = new FileInputStream(input)
      var in: ObjectInputStream = new ObjectInputStream(fs);
      var count = 0;
      while (fs.available() > 0) {
        try {
          if (count %10 == 0 && count != 0) {
            in = new ObjectInputStream(fs)
          }
          val obj = in.readObject()
          count += 1
          out_obj.writeObject(obj)
        } catch {
          case ex: java.io.StreamCorruptedException => print(ex + "\n")
        }
      }

      fs.close()
      in.close()
      out_obj.close()
      val end_time = System.nanoTime()

      (end_time - start_time)/1000/1000

    }

    def _compress(input: String, out: OutputStream): Long = {
      val start_time = System.nanoTime()
      val in_obj = new ObjectInputStream(new FileInputStream(input))
      val out_obj = new ObjectOutputStream(out)
      try {
        var count = 0
        while (true) {
            val obj = in_obj.readObject()
            count = count + 1
            // print("obj #" + count + " is a: " + obj.getClass() + ":\n")
            obj match {
              case tuple: Tuple2[_, _] =>
              // print(tuple._1.getClass + "\n")
              // print(tuple._2.getClass + "\n")
              case _ =>
            }
            // print(obj + ".toString(): " + obj + "\n")
            out_obj.writeObject(obj)
        }
        in_obj.close()
        out_obj.close()
      }
      catch {
        case ex: java.io.EOFException => print("found a unknown exception," + ex + "\n")
                                         in_obj.close()
                                         out_obj.close()
      }
      val end_time = System.nanoTime()

      (end_time - start_time)/1000/1000
    }

    def compress(input: String, compression_algorithm_name: String, output: String): Long = {

      val s = new FileOutputStream(output)

      var execution_time : Long = 0
      if (compression_algorithm_name == "zstd") {
        val level = 1
        val zstd_bufferSize = 32 * 1024
        val zstd_out = new BufferedOutputStream(new ZstdOutputStream(s, level), zstd_bufferSize)
        execution_time = _compress(input, zstd_out)

        // zstd_out.close()
      }

      if (compression_algorithm_name == "kaezip2") {
        val kaezip_bufferSize = 512
        val stream_id = 0;
        val kaezip_out = new KaezipOutputStream2(s, kaezip_bufferSize, stream_id)
        execution_time = _compress(input, kaezip_out)
      }

      if (compression_algorithm_name == "zlib") {
        val zlib_bufferSize = 512
        val zlib_out = new ZlibOutputStream(s, zlib_bufferSize)
        execution_time = _compress(input, zlib_out)
      }

      s.close()
      execution_time
    }

  def compress_rf(input: String, compression_algorithm_name: String, output: String): Long = {

    val s = new FileOutputStream(output)
    var out: OutputStream = null
    var execution_time : Long = 0
    if (compression_algorithm_name == "zstd") {
      val level = 1
      val zstd_bufferSize = 32 * 1024
      val zstd_out = new BufferedOutputStream(new ZstdOutputStream(s, level), zstd_bufferSize)
      execution_time = _compress_rf(input, zstd_out)
    }
    if (compression_algorithm_name == "kaezip2") {
      val kaezip_bufferSize = 32 * 1024
      val stream_id = 0
      val kaezip_out = new BufferedOutputStream(new KaezipOutputStream2(s, kaezip_bufferSize, stream_id),
                                                kaezip_bufferSize)
      execution_time = _compress_rf(input, kaezip_out)
    }
    if (compression_algorithm_name == "zlib") {
      val zlib_bufferSize = 512
      val zlib_out = new BufferedOutputStream(new ZlibOutputStream(s, zlib_bufferSize),
                                              zlib_bufferSize)
      execution_time = _compress_rf(input, zlib_out)
    }

    execution_time
  }
    def main(args: Array[String]): Unit = {
      val input = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId588-shuffleId0"
      val input2 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId798-shuffleId1"
      val input3 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId840-shuffleId2"
      val input4 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId21584-shuffleId99"
      val input5 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId21712-shuffleId100"
      val input6 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/svm/" +
        "shuffle_temp_file-mapId22294-shuffleId101"

      val kaezip_input = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/rf/rf_shuffle_kaezip_data" +
        "/shuffle_temp_file-mapId201-shuffleId0"

      //val input2 = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/lr/small/shuffle_temp_file-mapId635-shuffleId1"
      //readOrigFile(input6)

      val test_execution_time: ListBuffer[Long] = ListBuffer()
      // var test_execution_time: Long = 0
      val compression_algorithm_name = "zlib"
      val repeat_count = 11
      for (i <- 0 until repeat_count) {
         // remove the first test
         compress(input, compression_algorithm_name,
                  input + "." + compression_algorithm_name)

         test_execution_time += compress(input, compression_algorithm_name,
                                         input + "." + compression_algorithm_name)
         test_execution_time += compress(input2, compression_algorithm_name,
                                         input2 + "." + compression_algorithm_name)
         test_execution_time += compress(input3, compression_algorithm_name,
                                         input3 + "." + compression_algorithm_name)
         test_execution_time += compress(input4, compression_algorithm_name,
                                         input4 + "." + compression_algorithm_name)

         //System.out.println(test_execution_time)

      }
      System.out.print(compression_algorithm_name + ", compression time(ms)" + "," + test_execution_time.sum/test_execution_time.length + "\n")

      // uncompress(input)
    }
}// scalastyle:on println
