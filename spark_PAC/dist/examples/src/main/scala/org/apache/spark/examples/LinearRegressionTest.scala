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


import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scopt.OptionParser

object LinearRegressionWithElasticNet {

  def loadDatasets(input: String,
                   algo: String,
                   fracTest: Double): DataFrame = {
    val spark = SparkSession
      .builder
      .getOrCreate()

    // Load training data
    val data: RDD[LabeledPoint] = spark.sparkContext.objectFile(input)
    import spark.implicits._
    val origExamples = data.toDF()

    // Load or create test set
    val dataframes: Array[DataFrame] = origExamples.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)

    val training = dataframes(0).cache()

    val numTraining = training.count()

    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println("Loaded data:")
    println(s"  numTraining = $numTraining")
    println(s"  numFeatures = $numFeatures")
    training
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

    case class Params(
                       input: String = input,
                       regParam: Double = 0.3,
                       elasticNetParam: Double = 0.8,
                       maxIter: Int = maxIter.toInt,
                       tol: Double = 1E-6,
                       fracTest: Double = 0.25)
    val params = Params()

    val start_time = System.nanoTime()
    val spark = SparkSession
      .builder
      .appName("LinearRegression-" + compression_algorithm)
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

    val training: DataFrame = loadDatasets(params.input, "regression", params.fracTest)

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(params.maxIter)
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    val end_time = System.nanoTime()
    System.out.println("execution time(ms)," + (end_time - start_time)/1000/1000)

    spark.stop()
  }

}