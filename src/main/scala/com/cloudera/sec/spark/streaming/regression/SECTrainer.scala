package com.cloudera.sec.spark.streaming.regression

import com.cloudera.common.SEStreamingConstants
import onecloud.plantpower.database.driver.protobuf.Driver.FindDataPointResponse
import onecloud.plantpower.database.driver.protobuf.TSDBStruct.DataPoint
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by root on 6/12/17.
  */
class SECTrainer extends Serializable {

  def train(trainStream: DStream[(String, Array[Byte])], verifyStream: DStream[(String, Array[Byte])],prop:java.util.Properties): Unit = {
    val train = parse(trainStream,prop)
    val verify = parse(verifyStream,prop)
//    val input: DStream[LabeledPoint] = trainStream.flatMap( x => {
//      val response = FindDataPointResponse.parseFrom(x._2)
//      val pointList = response.getDataPointsList
//
//      val size = pointList.size()
//
//      println("pointList size =========="+size)
//      if (size > 0) {
//        val valueSize = pointList.get(0).getValuesList.size()
//        println("value size =========="+valueSize)
//        val lbArray = new Array[LabeledPoint](valueSize)
//        //val matrix = Array.ofDim[Double](size,valueSize)
//        val labelIndex = findLabelIndex(pointList, prop.getProperty(SEStreamingConstants.LABELNAME))
//
//        for (j <- 0 to (valueSize - 1)) {
//          var label = 0.0D
//          val tmpArray = new Array[Double](size - 1)
//          var tmpIndex = 0
//          for (i <- 0 to (size - 1)) {
//            println(pointList.get(i).getPoint.getCode)
//            if (i != labelIndex) {
//              tmpArray(tmpIndex) = pointList.get(i).getValues(j).getFloatValue.toDouble
//              tmpIndex = tmpIndex + 1
//            } else {
//              label = pointList.get(i).getValues(j).getFloatValue.toDouble
//            }
//            //matrix(j)(i) =  pointList.get(i).getValues(j).getFloatValue.toDouble
//          }
//          lbArray(j) = new LabeledPoint(label, Vectors.dense(tmpArray))
//
//        }
//        for(lp:LabeledPoint <- lbArray){
//          println("label= "+lp.label+"")
//          for(v:Double <- lp.features.toArray){
//            println("vector="+v)
//          }
//        }
//        lbArray
//      } else{
//        None
//      }
//    })

    val numFeatures = 8
    val stepSize = prop.getProperty(SEStreamingConstants.STEPSIZE).toDouble
    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures)).setStepSize(stepSize)
    model.trainOn(train)


    val predictStream = model.predictOnValues(verify.map(lp => (lp.label,lp.features)))
    predictStream.print()
    predictStream.foreachRDD({
      rdd => {
        //rdd.count()
        val valuesAndPreds = rdd.map(x => (x._2, x._1))
        if(valuesAndPreds.count() > 0) {
          val metrics = new RegressionMetrics(valuesAndPreds)
          println(s"MSE = ${metrics.meanSquaredError}")
          println(s"RMSE = ${metrics.rootMeanSquaredError}")
          // Mean absolute error
          println(s"MAE = ${metrics.meanAbsoluteError}")
          // Explained variance
          println(s"Explained variance = ${metrics.explainedVariance}")
        }
      }
    })


  }

//  def prepareVerificationData(trainStream: DStream[(String, Array[Byte])],prop:java.util.Properties): Unit ={
//    trainStream
//  }

  def findLabelIndex(pointList:java.util.List[DataPoint],labelName:String): Int ={
    var index = 0
    for(i <- 0 to (pointList.size -1)){
      if(labelName.equals(pointList.get(i).getPoint.getCode)) index = i
    }
    index
  }

  def parse(trainStream: DStream[(String, Array[Byte])],prop:java.util.Properties): DStream[LabeledPoint] ={

    val stream = trainStream.flatMap( x => {
      val response = FindDataPointResponse.parseFrom(x._2)
      val pointList = response.getDataPointsList

      val size = pointList.size()

      //println("pointList size ==========" + size)
      if (size > 0) {
        val valueSize = pointList.get(0).getValuesList.size()
        //println("value size ==========" + valueSize)
        val lbArray = new Array[LabeledPoint](valueSize)
        //val matrix = Array.ofDim[Double](size,valueSize)
        val labelIndex = findLabelIndex(pointList, prop.getProperty(SEStreamingConstants.LABELNAME))

        for (j <- 0 to (valueSize - 1)) {
          var label = 0.0
          val tmpArray = new Array[Double](size - 1)
          var tmpIndex = 0
          for (i <- 0 to (size - 1)) {
            //println(pointList.get(i).getPoint.getCode)
            if (i != labelIndex) {
              tmpArray(tmpIndex) = pointList.get(i).getValues(j).getFloatValue.toDouble
              tmpIndex = tmpIndex + 1
            } else {
              label = pointList.get(i).getValues(j).getFloatValue.toDouble
            }
            //matrix(j)(i) =  pointList.get(i).getValues(j).getFloatValue.toDouble
          }
          lbArray(j) = new LabeledPoint(label, Vectors.dense(tmpArray))

        }
//        for (lp: LabeledPoint <- lbArray) {
//          println("label= " + lp.label + "")
//          for (v: Double <- lp.features.toArray) {
//            println(v)
//          }
//        }
        lbArray
      } else {
        None
      }
    })
    stream
  }


  def evulate(labelCol:String): Unit ={
    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("predicition")
  }


}
