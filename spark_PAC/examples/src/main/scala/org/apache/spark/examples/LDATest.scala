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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, LocalLDAModel}
// import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scopt.OptionParser

/**
 * Usage: SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]
 */
object LDATest {


  def main(args: Array[String]) {

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
    val maxIterations = args(11).toInt

    case class Params(
                       inputPath: String = input,
                       outputPath: String = output,
                       numTopics: Int = 5,
                       maxIterations: Int = maxIterations.toInt,
                       optimizer: String = "em",
                       maxResultSize: String = "1g")

    val params = Params()

    val start_time = System.nanoTime()
    val spark = SparkSession
      .builder
      .appName("LDA-" + compression_algorithm)
      .config("spark.driver.maxResultSize", params.maxResultSize)
      .config("spark.io.compression.codec", compression_algorithm)
      .config("spark.shuffle.compress", compression_flag)
      .config("spark.kaezip.buffersize", compression_buffersize)
      .config("spark.zlib.buffersize", compression_buffersize)
      .config("spark.shufflewrite.optimization", compression_write_opti)
      .config("spark.shuffleread.optimization", compression_read_opti)
      .config("spark.mylogger.flag", mylogger_flag)
      .config("spark.compression.performance.trace", performance_trace)
      .config("spark.shuffle.compression.raw.sample", raw_sample)
      .config("spark.shuffle.files.save", shuffle_file_save)
      //.master(master = "local[6]")
      .getOrCreate()

    System.out.println(shuffle_file_save)
    val sc = spark.sparkContext

    val corpus: RDD[(Long, Vector)] = sc.objectFile(params.inputPath)

    // Cluster the documents into numTopics topics using LDA
    val ldaModel = new LDA().setK(params.numTopics).setMaxIterations(params.maxIterations).setOptimizer(params.optimizer).run(corpus)

    // Save and load model.
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, params.numTopics)) {
      print(s"Topic $topic :")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(s"${topics(word, topic)}")
      }
      println()
    }

    ldaModel.save(sc, params.outputPath)
    //val savedModel = LocalLDAModel.load(sc, params.outputPath)

    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)


    sc.stop()
  }
}

// scalastyle:on println
