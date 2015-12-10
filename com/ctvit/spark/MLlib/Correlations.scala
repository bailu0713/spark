package com.ctvit.spark.MLlib

/**
 * Created by BaiLu on 2015/9/24.
 */
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

import scopt.OptionParser

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}


/*
 *
 * An example app for summarizing multivariate data from a file. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.Correlations
 * }}}
 * By default, this loads a synthetic dataset from `data/mllib/sample_linear_regression_data.txt`.
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object Correlations {

  case class Params(input: String = "/user/bl/mllib/sample_linear_regression_data.txt")
    extends AbstractParams[Params]

  def main(args: Array[String]) {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Correlations") {
      head("Correlations: an example app for computing correlations")
      opt[String]("input")
        .text(s"Input path to labeled examples in LIBSVM format, default: ${defaultParams.input}")
        .action((x, c) => c.copy(input = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          | bin/spark-submit --class org.apache.spark.examples.mllib.Correlations \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --input data/mllib/sample_linear_regression_data.txt
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"Correlations with $params")
    val sc = new SparkContext(conf)

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()
    //-0.1829397047693725 1:0.09377558075225512 2:0.5774384503027374 3:-0.7104684187448009 4:-0.07285914169135976 5:-0.8797920488335114 6:0.6099615504974201 7:-0.8047440624324915 8:-0.6877856114263066 9:0.5843004021777447 10:0.5190581455348131
    //18.479680552020344 1:0.9635517137863321 2:0.9954507816218203 3:0.11959899129360774 4:0.3753283274192787 5:-0.9386713095183621 6:0.0926833703812433 7:0.48003949462701323 8:0.9432769781973132 9:-0.9637036991931129 10:-0.4064407447273508
    //1.3850645873427236 1:0.14476184437006356 2:-0.11280617018445871 3:-0.4385084538142101 4:-0.5961619435136434 5:0.419554626795412 6:-0.5047767472761191 7:0.457180284958592 8:-0.9129360314541999 9:-0.6320022059786656 10:-0.44989608519659363

    println(s"Summary of data file: ${params.input}")
    println(s"${examples.count()} data points")

    // Calculate label -- feature correlations
    val labelRDD = examples.map(_.label)

//    -6.606325361718908
//    1.7362099941626894
//    -15.359544879832677
//    -1.0349488340235453
    val numFeatures = examples.take(1)(0).features.size
    val corrType = "pearson"
    val features=examples.map(_.features)
    println(s"Correlation ($corrType) between label and each feature")
    println(s"Feature\tCorrelation")
    var feature = 0
    for(feature<- 0 until numFeatures){
//    while (feature < numFeatures) {
//      examples.map(_.features(feature))这句该怎么理解呢？？？？？？？？？？？？？
//      返回第一个值为feature的个数，第二个array是每个属性的索引，第三个是对应索引的value
//      (10,[0,1,2,3,4,5,6,7,8,9],[0.4551273600657362,0.36644694351969087,-0.38256108933468047,-0.4458430198517267,0.33109790358914726,0.8067445293443565,-0.2624341731773887,-0.44850386111659524,-0.07269284838169332,0.5658035575800715])
//      (10,[0,1,2,3,4,5,6,7,8,9],[0.8386555657374337,-0.1270180511534269,0.499812362510895,-0.22686625128130267,-0.6452430441812433,0.18869982177936828,-0.5804648622673358,0.651931743775642,-0.6555641246242951,0.17485476357259122])
      val featureRDD = examples.map(_.features(feature))
      val corr = Statistics.corr(labelRDD, featureRDD)
//      println("featureRDD")
      println(s"$feature\t$corr")
//      feature += 1
    }
    println()

    sc.stop()
  }
}
