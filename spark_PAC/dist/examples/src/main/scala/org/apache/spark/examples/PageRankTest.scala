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

import org.apache.spark.sql.SparkSession

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 */
object PageRankTest {

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
    val input = args(9)
    val output = args(10)
    val maxIter = args(11).toInt

    case class Params(inputPath: String = input,
                       outputPath: String = output,
                       iter_num: Int = maxIter.toInt
                     )
    val params = Params()

    val start_time = System.nanoTime()

    val spark = SparkSession
      .builder
      .appName("Pagerank-" + compression_algorithm)
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
      //.master(master = "local[8]")
      .getOrCreate()


    val iters = params.iter_num
    val lines = spark.read.textFile(params.inputPath).rdd
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    ranks.saveAsTextFile(params.outputPath)
    // val output = ranks.collect()
    // output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)

    spark.stop()
  }
}
// scalastyle:on println
