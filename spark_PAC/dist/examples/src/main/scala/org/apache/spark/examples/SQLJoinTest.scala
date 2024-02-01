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
package org.apache.spark.examples

import org.slf4j.LoggerFactory

// $example on:programmatic_schema$
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$

object SQLJoinTest {

  def main(args: Array[String]): Unit = {

    val compression_algorithm = args(0)
    val compression_buffersize = args(1).toInt
    val compression_flag = args(2)
    val compression_write_opti = args(3)
    val compression_read_opti = args(4)
    val performance_trace = args(5)
    val mylogger_flag = args(6)
    val raw_sample = args(7)
    val shuffle_file_save = args(8)
    val big_table = args(9)
    val small_table = args(10)

    case class Params(
                       big_table: String = big_table,
                       small_table: String = small_table
                     )
    val params = Params()

    val start_time = System.nanoTime()

    val spark = SparkSession
      .builder
      .appName("SQLJoin-" + compression_algorithm)
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
      //.master(master = "local[6]")
      .getOrCreate()

    val schema = new StructType()
      .add("start_node", StringType, true)
      .add("end_node", StringType, true)

    val df_big_table = spark.read.schema(schema).options(Map("delimiter" -> "\t")).csv(params.big_table)
    // df_big_table.show()
    val df_small_table = spark.read.schema(schema).options(Map("delimiter" -> "\t")).csv(params.small_table)
    // df_small_table.show()
    val cnt = df_big_table.join(df_small_table, df_big_table("start_node") === df_small_table("start_node"), "inner").count()

    System.out.println(cnt)
    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time) / 1000 / 1000)

    spark.stop()

  }
}
