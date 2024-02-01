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

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{LDA, LocalLDAModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
// import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{HashMap => MHashMap}

import java.util.Random
/**
 * Usage: SimpleSkewedGroupByTest [numMappers] [numKVPairs] [valSize] [numReducers] [ratio]
 */
object LDAGenerator {
  def generateLDARDD(
                      sc: SparkContext,
                      numDocs: Long,
                      numVocab: Int,
                      docLenMin: Int,
                      docLenMax: Int,
                      numParts: Int = 3,
                      seed: Long = System.currentTimeMillis()): RDD[(Long, Vector)] = {
    val data = sc.parallelize(0L until numDocs, numParts).mapPartitionsWithIndex {
      (idx, part) =>
        val rng = new Random(seed ^ idx)
        part.map { case docIndex =>
          var currentSize = 0
          val entries = MHashMap[Int, Int]()
          val docLength = rng.nextInt(docLenMax - docLenMin + 1) + docLenMin
          while (currentSize < docLength) {
            val index = rng.nextInt(numVocab)
            entries(index) = entries.getOrElse(index, 0) + 1
            currentSize += 1
          }

          val iter = entries.toSeq.map(v => (v._1, v._2.toDouble))
          (docIndex, Vectors.sparse(numVocab, iter))
        }
    }
    data
  }

  def main(args: Array[String]) {
    var outputPath = "hdfs://localhost:9000/lda"
    var numDocs: Long = 5000L
    var numVocab: Int = 1000
    var docLenMin: Int = 50
    var docLenMax: Int = 10000
    val numPartitions = 8


    val start_time = System.nanoTime()
    val spark = SparkSession
      .builder
      .appName("LDADataGenerator")
      .master(master = "local[4]")
      .getOrCreate()

    val sc = spark.sparkContext
    val data = generateLDARDD(sc, numDocs, numVocab, docLenMin, docLenMax, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}

// scalastyle:on println
