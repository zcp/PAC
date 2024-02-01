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

import breeze.linalg.{squaredDistance, DenseVector, Vector}

import org.apache.spark.sql.SparkSession

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.clustering.KMeans.
 */
object KMeansTest {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def showWarning(): Unit = {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use org.apache.spark.ml.clustering.KMeans
        |for more conventional use.
      """.stripMargin)
  }

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
    val maxIter = args(10).toInt

    val start_time = System.nanoTime()

    // val input = "hdfs://localhost:9000/1G"
    val spark = SparkSession
      .builder
      .appName("Kmeans-" + compression_algorithm)
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
      //.master(master = "local[4]")
      .getOrCreate()

    val lines = spark.read.textFile(input).rdd
    val data = lines.map(parseVector _).cache()
    val K = 10
    val convergeDist = 0.001

    val kPoints = data.takeSample(withReplacement = false, K, 42)
    var tempDist = 1.0

    var iter_num = 0
    while(tempDist > convergeDist && iter_num < maxIter.toInt) {
      val closest = data.map (p => (closestPoint(p, kPoints), (p, 1)))

      val pointStats = closest.reduceByKey{case ((p1, c1), (p2, c2)) => (p1 + p2, c1 + c2)}

      val newPoints = pointStats.map {pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2))}.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        // println(s"$i")
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
      println(s"Finished iteration (delta = $tempDist)")

      iter_num = iter_num + 1
    }

    println("Final centers:")
    kPoints.foreach(println)

    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)

    spark.stop()
  }
}

// scalastyle:on println
