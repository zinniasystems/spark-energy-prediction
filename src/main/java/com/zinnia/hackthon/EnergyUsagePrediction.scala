package com.zinnia.hackthon

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.util.Random

/**
 * Author: madhu
 * Class to predict for first and second problem
 */
class EnergyUsagePrediction(sparkContext: SparkContext) {

  private def dailyAggregation(inputRDD: RDD[((String, Long), Record)]): RDD[(Date, Record)] = {
    inputRDD.map(value => {
      val ((date, hour), record) = value
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
      val dateValue = dateFormat.parse(date)
      (dateValue, record)
    }).reduceByKey((firstRecord, secondRecord) => {
      val record = new Record()
      record.hourofDay = firstRecord.hourofDay
      record.subMetering1 = firstRecord.subMetering1 + secondRecord.subMetering1
      record.subMetering2 = firstRecord.subMetering2 + secondRecord.subMetering2
      record.subMetering3 = firstRecord.subMetering3 + secondRecord.subMetering3

      record.activePower = firstRecord.activePower + secondRecord.activePower
      record.reactivePower = firstRecord.reactivePower + secondRecord.reactivePower

      record.voltage = (firstRecord.voltage + secondRecord.voltage) / 2
      record.globalIntensity = (firstRecord.globalIntensity + secondRecord.globalIntensity) / 2
      record
    })


  }

  private def standardMean(inputRDD: RDD[(Date, Record)]): (List[Double], Double) = {
    val count = inputRDD.count()
    var sum = 0.0
    var riList = List[Double]()
    for (i <- Range(1, count.toInt)) {
      val firstRecord = inputRDD.toArray()(i)
      val secondRecord = inputRDD.toArray()(i - 1)
      val difference = (firstRecord._2.totalPowerUsed - secondRecord._2.totalPowerUsed) / firstRecord._2.totalPowerUsed
      riList = riList ::: List(difference)
      sum += difference
    }

    (riList, sum / count)

  }

  private def standDeviation(inputRDD: RDD[Double], mean: Double): Double = {
    val sum = inputRDD.map(value => {
      (value - mean) * (value - mean)
    }).reduce((firstValue, secondValue) => {
      firstValue + secondValue
    })
    scala.math.sqrt(sum / inputRDD.count())
  }

  def getWeeklyEnergyConsumption(inputRDD: RDD[Record]):List[String] = {

    def weeklyAggregation(predictedValue: Double, mean: Double, stdDeviation: Double, count: Double, lastvalue: Double, time: Double): (Double, Double, Double, Double, Double, Double) = {
      val r1 = (predictedValue - lastvalue) / lastvalue
      val updatedMean: Double = (mean * count + r1) / (count + 1)
      var stdSum: Double = stdDeviation * stdDeviation * count
      stdSum = stdSum + (updatedMean - r1) * (updatedMean - r1)
      val updatedStd: Double = scala.math.sqrt(stdSum / (count + 1))
      val random = new Random()
      val newPredicted = lastvalue * (1 + updatedMean * time + updatedStd * random.nextDouble() * scala.math.sqrt(time))
      (newPredicted, updatedMean, updatedStd, count + 1, newPredicted, time + 1)
    }


    val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val dataAggregator = new DataAggregator()
    val predictatedValue = getNextDayEnergyConsumption(inputRDD)
    val hourlyAggregatedRDD = dataAggregator.hourlyAggregator(inputRDD)
    val dailyAgreegatedRDD = dailyAggregation(hourlyAggregatedRDD)
    val sortedRDD = dailyAgreegatedRDD.sortByKey(false)
    val (riList, mean) = standardMean(sortedRDD)
    val stdDeviation = standDeviation(sparkContext.makeRDD(riList), mean)
    val calendar = Calendar.getInstance()
    calendar.setTime(sortedRDD.toArray() {
      0
    }._1)
    var finalArray = List[String]()
    val totalCount = sortedRDD.count()
    for (k <- Range(0, 52)) {

      val array = new Array[Array[Double]](1000, 7)
      for (i <- Range(0, 999)) {
        var tempSet = weeklyAggregation(predictatedValue, mean, stdDeviation, totalCount, predictatedValue, 1)
        for (j <- Range(0, 7)) {
          tempSet = weeklyAggregation(tempSet._1, tempSet._2, tempSet._3, tempSet._4, tempSet._5, tempSet._6)
          array(i)(j) = tempSet._1
        }
      }
      for (i <- Range(0, 7)) {
        calendar.add(Calendar.DATE, 1)
        val predictedArray = new Array[Double](1000)
        for (j <- Range(0, 999)) {
          predictedArray(j) = array(j)(i)
        }

        val sortedArray = predictedArray.sorted
        finalArray = finalArray ::: List(k + "," + i + "," + sortedArray {
          10
        } + "," + simpleDateFormat.format(calendar.getTime()))

      }

    }
    finalArray
  }


  def getNextDayEnergyConsumption(inputRDD: RDD[Record]) = {
    /**
     * Predicting using Geometric_Brownian_motion
     */

    val dataAggregator = new DataAggregator()
    val hourlyAggregatedRDD = dataAggregator.hourlyAggregator(inputRDD)
    val dailyAgreegatedRDD = dailyAggregation(hourlyAggregatedRDD)
    val sortedRDD = dailyAgreegatedRDD.sortByKey(false)
    val (riList, mean) = standardMean(sortedRDD)
    val stdDeviation = standDeviation(sparkContext.makeRDD(riList), mean)
    val lastValue = sortedRDD.toArray()(0)._2.totalPowerUsed
    val date = sortedRDD.toArray()(0)._1
    var predictedValues = List[Double]()
    for (i <- Range(1, 1000)) {
      val predictedValue = lastValue * (1 + mean * 1 + stdDeviation * scala.math.random * 1)
      predictedValues ::= predictedValue
    }
    predictedValues = predictedValues.sorted
    val predictatedValue = predictedValues(10)
    val predictedValueKW = predictatedValue / 1000
    predictedValueKW

  }


}

