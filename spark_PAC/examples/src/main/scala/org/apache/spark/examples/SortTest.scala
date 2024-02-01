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
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

/**
 * Usage: SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]
 */
object SortTest {
  def main(args: Array[String]) {
    // val compression_algorithm = "kaezip"

    val compression_algorithm = args(0)
    val compression_buffersize = args(1).toInt
    val compression_flag = args(2)
    val compression_write_opti = args(3)
    val compression_read_opti = args(4)
    val performance_trace = args(5)
    val mylogger_flag = args(6)
    val raw_sample = args(7)
    val shuffle_file_save = args(8)
    val input = args(9)

    val start_time = System.nanoTime()
    val spark = SparkSession
      .builder
      .appName("Sort-" + compression_algorithm)
      .config("spark.io.compression.codec", compression_algorithm)
      .config("spark.shuffle.compress", compression_flag)
      .config("spark.kaezip.buffersize", compression_buffersize)
      .config("spark.zlib.buffersize", compression_buffersize)
      .config("spark.shufflewrite.optimization", compression_write_opti)
      .config("spark.shuffleread.optimization", compression_read_opti)
      .config("spark.compression.performance.trace", performance_trace)
      .config("spark.mylogger.flag", mylogger_flag)
      .config("spark.shuffle.compression.raw.sample", raw_sample)
      .config("spark.shuffle.files.save", shuffle_file_save)
      .master(master = "local[4]")
      .getOrCreate()

    System.out.println(compression_algorithm, compression_buffersize,
      compression_read_opti, compression_write_opti, performance_trace,
      mylogger_flag, raw_sample, shuffle_file_save)
    // val input = "hdfs://localhost:9000/HiBench/Wordcount/Input"
    val sortedWords = spark.read.textFile(input).rdd.map((_, 1)).sortByKey()

    println(sortedWords.count())
    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)
    spark.stop()
  }
}

// scalastyle:on println
