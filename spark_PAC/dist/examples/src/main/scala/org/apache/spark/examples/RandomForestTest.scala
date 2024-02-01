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

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object RandomForestTest {

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

    case class Params(
                       inputPath: String = input,
                       numTrees: Int = 3,
                       numClasses: Int = 2,
                       featureSubsetStrategy: String = "auto",
                       impurity: String = "gini",
                       maxDepth: Int = 4,
                       maxBins: Int = 32)

    val start_time = System.nanoTime()
    val params = Params()

    val spark = SparkSession
          .builder
          .appName("RandomForest-" + compression_algorithm)
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
          //.master(master = "local[4]")
          .getOrCreate()

    val sc = spark.sparkContext

    // $example on$
    // Load and parse the data file.
    val data: RDD[LabeledPoint] = sc.objectFile(params.inputPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.

    val categoricalFeaturesInfo = Map[Int, Int]()

    val model = RandomForest.trainClassifier(trainingData, params.numClasses,
                                             categoricalFeaturesInfo,
                                             params.numTrees,
                                             params.featureSubsetStrategy,
                                             params.impurity,
                                             params.maxDepth, params.maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
          val prediction = model.predict(point.features)
          (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)

    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)

    sc.stop()
      }
    }
// scalastyle:on println