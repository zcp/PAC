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

import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object SVMWithSGDExample {

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

    case class Params(
                       numIterations: Int = maxIter.toInt,
                       stepSize: Double = 1.0,
                       regParam: Double = 0.01,
                       dataPath: String = input,
                       storageLevel: String = "MEMORY_ONLY"
                     )

    val params = Params()

    val start_time = System.nanoTime()

    val spark = SparkSession
          .builder
          .appName("SVMRegression-" + compression_algorithm)
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

    val dataPath = params.dataPath
    val numIterations = params.numIterations
    val stepSize = params.stepSize
    val regParam = params.regParam
    val storageLevel = StorageLevel.fromString(params.storageLevel)

    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).persist(storageLevel)
    val test = splits(1)

    // Run training algorithm to build the model
    val model = SVMWithSGD.train(training, numIterations, stepSize, regParam)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)

    sc.stop()
  }
}
// scalastyle:on println