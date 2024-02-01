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

package com.github.ehiggs.spark.terasort

import java.util.Comparator
import com.google.common.primitives.UnsignedBytes
import org.apache.spark.{SparkConf, SparkContext}


/**
 * This is a great example program to stress test Spark's shuffle mechanism.
 *
 * See http://sortbenchmark.org/
 */
object TeraSort {

  implicit val caseInsensitiveOrdering : Comparator[Array[Byte]] =
    UnsignedBytes.lexicographicalComparator

  def main(args: Array[String]) {

    val compression_algorithm = args(0)
    val compression_buffersize = args(1)
    val compression_flag = args(2)
    val compression_write_opti = args(3)
    val compression_read_opti = args(4)
    val performance_trace = args(5)
    val mylogger_flag = args(6)
    val raw_sample = args(7)
    val shuffle_file_save = args(8)
    val inputFile = args(9)
    val outputFile = args(10)

    val conf = new SparkConf()
      .setAppName(s"TeraSort" + compression_algorithm)
      .set("spark.io.compression.codec", compression_algorithm)
      .set("spark.shuffle.compress", compression_flag)
      .set("spark.kaezip.buffersize", compression_buffersize)
      .set("spark.zlib.buffersize", compression_buffersize)
      .set("spark.shufflewrite.optimization", compression_write_opti)
      .set("spark.shuffleread.optimization", compression_write_opti)
      .set("spark.compression.performance.trace", performance_trace)
      .set("spark.mylogger.flag", mylogger_flag)
      .set("spark.shuffle.compression.raw.sample", raw_sample)
      .set("spark.shuffle.files.save", shuffle_file_save)

    val sc = new SparkContext(conf)


    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    val startTime = System.currentTimeMillis()
    val sorted = dataset.repartitionAndSortWithinPartitions(
      new TeraSortPartitioner(dataset.partitions.length))
    sorted.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)
    val endTime = System.currentTimeMillis()
    println(s"==== TeraSort took ${(endTime-startTime)/1000.0}s ====")
    sc.stop()
  }
}
