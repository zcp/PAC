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
package org.apache.spark.examples.sql

// import org.apache.spark._

import java.io.{File, BufferedInputStream, ByteArrayInputStream, DataInputStream, FileInputStream, InputStream, ObjectInputStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.UnsafeRowSerializer
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}
import com.google.common.io.ByteStreams
import org.apache.spark.io.KaeInflaterInputStream


class ClosableByteArrayInputStream(buf: Array[Byte]) extends ByteArrayInputStream(buf) {
  var closed: Boolean = false
  override def close(): Unit = {
    closed = true
    super.close()
  }
}


object SparkSQLExample2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local")
      .appName("SparkApp")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.mb", "24")
      .getOrCreate()

    /*
    val df = spark.read.json("/home/zcp/kunpeng/sql_join_problem/test/custom_person.json")
    df.createOrReplaceTempView("t1")

    val internalRowRdd: RDD[InternalRow] = df.queryExecution.toRdd

    internalRowRdd.map(internalRow => {
      val unsafeRow: UnsafeRow = internalRow.asInstanceOf[UnsafeRow]

      val numFields = unsafeRow.numFields
      val sizeInBytes = unsafeRow.getSizeInBytes
      val baseObject = unsafeRow.getBytes
      val baseOffset = unsafeRow.getBaseOffset


      print(",Num fields," + numFields)
      print(",Size in bytes," + sizeInBytes)
      print(",base offset, " + baseOffset)
      print(",unsafe row to string," + unsafeRow.toString)

      for (i <- Seq(8, 16)) {
        if (i > 0) {
          print(",int value," + Platform.getInt(baseObject, baseOffset + i))
        } else {
          print(",hex string value," + java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)))
        }
      }

    }).count()
  */


   /*
    def deserializeStream(in: InputStream): DeserializationStream = {
      val numFields = 2;
      val rowSize = 24;
      new DeserializationStream {
        private[this] val dIn: DataInputStream = new DataInputStream(new BufferedInputStream(in))
        // 1024 is a default buffer size; this buffer will grow to accommodate larger rows
        private[this] var rowBuffer: Array[Byte] = new Array[Byte](1024)
        private[this] var row: UnsafeRow = new UnsafeRow(numFields)

        override def next(): (Int, UnsafeRow) = {
          if (rowBuffer.length < rowSize) {
            rowBuffer = new Array[Byte](rowSize)
          }
          ByteStreams.readFully(dIn, rowBuffer, 0, rowSize)
          row.pointTo(rowBuffer, Platform.BYTE_ARRAY_OFFSET, rowSize)
          rowSize = readSize()
          if (rowSize == EOF) { // We are returning the last row in this stream
            dIn.close()
            val _rowTuple = rowTuple
            // Null these out so that the byte array can be garbage collected once the entire
            // iterator has been consumed
            row = null
            rowBuffer = null
            rowTuple = null
            _rowTuple
          } else {
            rowTuple
          }
        }
      }
    }
    */
    // test(spark)
    val dir_path = "/home/zcp/kunpeng/sql_join_problem/none2"
    val record_num = sum_records(dir_path)
    print("obj num," + record_num + "\n");
    spark.stop()
  }

  def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith("_1"))
      .map(_.getPath).toList
  }

  def sum_records(dirName: String): Int = {
    var total_count = 0
    val files = getListOfFiles(dirName)
    for(input <- files) {
      val serializer = new UnsafeRowSerializer(numFields = 3).newInstance()
      val kae_in = new FileInputStream(input)
      val deserializerIter = serializer.deserializeStream(kae_in).asKeyValueIterator

      while(deserializerIter.hasNext) {
        val (_, _) = deserializerIter.next()
        total_count = total_count + 1
      }
      kae_in.close()
    }

    total_count
  }

  def uncompress(input: String): Unit = {
    // val filePath = "/home/zcp/kunpeng/kaezip_lda/13589327364703_" +
    //                "temp_shuffle_fb9bcfc2-07ed-43d8-8b61-dc835fad3ba1"

    val filePath2 = "/home/zcp/kunpeng/kaezip_lda/1358_ser.orig"
    val filePath3 = "/home/zcp/kunpeng/kaezip_lda/3371393487424160_" +
                    "temp_shuffle_1c358a9e-9530-4cf4-8d6d-f9da4bea070b.zlib"

    val kae_in = new KaeInflaterInputStream(new FileInputStream(input));
    val in_obj = new ObjectInputStream(kae_in)

    try {
      var count = 0
      while (true) {
        val obj = in_obj.readObject()
        count = count + 1
        // println("obj #" + count + " is a: " + obj.getClass())
        // println(obj + ".toString(): " + obj)
      }
      in_obj.close()
    } catch {
      case ex: Throwable => print("found a unknown exception," + ex + "\n");
                            in_obj.close()
    }
  }

  def test(spark: SparkSession): Unit = {
    /*
    val df = spark.read.json("/home/zcp/kunpeng/sql_join_problem/test/custom_person.json")
    df.createOrReplaceTempView("t1")

    val internalRowRdd: RDD[InternalRow] = df.queryExecution.toRdd
    val serializer = new UnsafeRowSerializer(numFields = 2).newInstance()
    val baos = new ByteArrayOutputStream()
    val serializerStream = serializer.serializeStream(baos)

    val unsafeRows = internalRowRdd.map(internalRow => {
                 val row = internalRow.asInstanceOf[UnsafeRow]
                 serializerStream.writeKey(0)
                 serializerStream.writeValue(row)
                 row
                })
    unsafeRows.collect()
    serializerStream.close()
   */
    val serializer = new UnsafeRowSerializer(numFields = 3).newInstance()
    val input = "/home/zcp/kunpeng/sql_join_problem/test/raw_data6"
    val kae_in = new FileInputStream(input)
    // val kae_in = new KaeInflaterInputStream(new FileInputStream(input));
    // val baos = new ByteArrayInputStream()
    // val input = new ClosableByteArrayInputStream(baos.toByteArray)
    val deserializerIter = serializer.deserializeStream(kae_in).asKeyValueIterator
    print("hello world")

    var count = 0
    while(deserializerIter.hasNext) {
      // val expectedRow = deserializerIter.next().asInstanceOf[UnsafeRow]
      val (obj, obj2) = deserializerIter.next()
      val expectedRow = obj2.asInstanceOf[UnsafeRow]
      print("obj #" + count + " is a: " + obj.getClass() + "," + expectedRow.getClass() + "\n");
      val numFields = expectedRow.numFields
      val sizeInBytes = expectedRow.getSizeInBytes
      val baseObject = expectedRow.getBytes
      val baseOffset = expectedRow.getBaseOffset

      print("Num fields," + numFields)
      print(",Size in bytes," + sizeInBytes)
      print(",base offset, " + baseOffset)
      print(",unsafe row to string," + expectedRow.toString)

      for (i <- Seq(8, 16)) {
        if (i > 0) {
          print(",int value," + Platform.getInt(baseObject, baseOffset + i))
        } else {
          print(",hex string value," + java.lang.Long.toHexString(Platform.getLong(baseObject, baseOffset + i)))
        }
      }
      print("\n")
      count = count + 1
      /*
      val expectedRow = deserializerIter.next().getClass()
      val numFields = expectedRow.numFields
      val sizeInBytes = expectedRow.getSizeInBytes
      val baseObject = expectedRow.getBytes
      val baseOffset = expectedRow.getBaseOffset

      print(",Num fields," + numFields)
      print(",Size in bytes," + sizeInBytes)
      print(",base offset, " + baseOffset)
      print(",unsafe row to string," + expectedRow.toString)

       */
    }
    kae_in.close()
    // input.close()
  }
}