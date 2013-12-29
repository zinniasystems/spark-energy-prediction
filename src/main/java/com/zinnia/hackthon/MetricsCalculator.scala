package com.zinnia.hackthon

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import java.text.SimpleDateFormat

/**
 * Author: madhu
 *
 */

/**
 * Calculates the revenue loss for a outage per day
 */
class MetricsCalculator(sparkContext:SparkContext) {

  def getPerHourRevenue(inputRDD:RDD[Record]):RDD[Record] = {
    val dataAggregator = new DataAggregator()
    val hourlyRDD = dataAggregator.hourlyAggregator(inputRDD)
    hourlyRDD.map(value => {
      var ((date, hour), record) = value
      val hourOfDay = record.hourofDay
      if ((hourOfDay >= 0 && hourOfDay <= 4) || (hourOfDay >= 10 && hourOfDay <= 15)) {
        record.totalCost = (record.subMetering1 * 4 + record.subMetering2 * 4 + record.subMetering3 * 4) / 1000
      } else if ((hourOfDay >= 5 && hourOfDay <= 6) || (hourOfDay >= 16 && hourOfDay <= 19) || (hourOfDay >= 22 && hourOfDay <= 23)) {
        record.totalCost = (record.subMetering1 * 6 + record.subMetering2 * 6 + record.subMetering3 * 6) / 1000
      } else if (hourOfDay >= 7 && hourOfDay <= 9) {
        record.totalCost = (record.subMetering1 * 12 + record.subMetering2 * 12 + record.subMetering3 * 12) / 1000
      } else {
        record.totalCost = (record.subMetering1 * 10 + record.subMetering2 * 10 + record.subMetering3 * 10) / 1000
      }

      record
    })
  }

  def getRevenueLoss(inputRDD:RDD[Record]):Double = {
    val dataAggregator = new DataAggregator()
    val revenueRDD= getPerHourRevenue(inputRDD)
    val dailyRDD = dataAggregator.dailyAggregator(revenueRDD)
    val totalCostCalcRDD = dailyRDD.map(value => ("sum", value._2.totalCost)).reduceByKey((a, b) => a + b)
    val revenueLossForDayOutage = totalCostCalcRDD.first()._2 / dailyRDD.count()
    revenueLossForDayOutage

  }

  def getPeekLoad(inputRDD:RDD[Record]):(Double,Double)  = {
    def findNextMonthPeakLoad(rdd:RDD[((Int,Long),Record)],sparkContext:SparkContext) : Double={
      def standardMean(inputRDD: RDD[((Int,Long),Record)]): (List[Double], Double) = {
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

      def standDeviation(inputRDD:RDD[Double],mean:Double): Double = {
        val sum = inputRDD.map(value => {
          (value - mean) * (value - mean)
        }).reduce((firstValue, secondValue) => {
          firstValue + secondValue
        })
        scala.math.sqrt(sum / inputRDD.count())
      }


      val (rList,mean) = standardMean(rdd)
      val stdDeviation = standDeviation(sparkContext.makeRDD(rList),mean)
      val sortedRdd=rdd.sortByKey(false)
      val lastValue = sortedRdd.first()._2.totalPowerUsed
      var newValues = List[Double]()
      for(i<- Range(0,1000)){
        val predictedValue = lastValue * (1 + mean * 1 + stdDeviation * scala.math.random * 1)
        newValues::=predictedValue
      }
      val sorted = newValues.sorted
      val value = sorted(10)/1000
      value
    }

    def peakTimeRecords(inputRDD: RDD[Record]): RDD[Record] = {
      inputRDD.filter(record => (record.hourofDay >= 7 && record.hourofDay <= 10))
    }


    def weekendPeakTimeRecords(inputRDD: RDD[Record]): RDD[Record] = {
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
      inputRDD.filter(record => ((dateFormat.parse(record.date).getDay == 0) || (dateFormat.parse(record.date).getDay == 6)))
    }

    def weekdayPeakTimeRecords(inputRDD: RDD[Record]): RDD[Record] = {
      val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
      inputRDD.filter(record => !((dateFormat.parse(record.date).getDay == 0) || (dateFormat.parse(record.date).getDay == 6)))
    }

    val dataAggregator = new DataAggregator()
    val revenueRDD = getPerHourRevenue(inputRDD)
    var peakTimeRecordRDD = peakTimeRecords(revenueRDD)
    var weekendPeakTimeRecord = weekendPeakTimeRecords(peakTimeRecordRDD)
    var weekdayPeakTimeRecord = weekdayPeakTimeRecords(peakTimeRecordRDD)
    var weekendPeakTimeMonthlyRDD = dataAggregator.monthlyAggregator(weekendPeakTimeRecord)
    var weekdayPeakTimeMonthlyRDD = dataAggregator.monthlyAggregator(weekdayPeakTimeRecord)
    val peakTimeLoadWeekday = findNextMonthPeakLoad(weekdayPeakTimeMonthlyRDD, sparkContext)
    val peakTimeLoadWeekend = findNextMonthPeakLoad(weekendPeakTimeMonthlyRDD, sparkContext)
    (peakTimeLoadWeekday,peakTimeLoadWeekend)

  }



}
