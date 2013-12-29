package com.zinnia.hackthon

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

/**
 * Author: madhu
 * Aggregates the data on hour,day and month
 *
 */
class DataAggregator {

  def hourlyAggregator(inputRDD: RDD[Record]): RDD[((String, Long), Record)] = {
    val groupRDD = inputRDD.map(record => ((record.date, record.hourofDay), record)).reduceByKey((firstRecord, secondRecord) => {
      val record = new Record()

      record.date = firstRecord.date
      record.day = firstRecord.day
      record.month = firstRecord.month
      record.year = firstRecord.year
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
    groupRDD


  }

  def dailyAggregator(inputRDD: RDD[Record]): RDD[(String, Record)] = {
    val groupRDD = inputRDD.map(record => (record.date, record)).reduceByKey((firstRecord, secondRecord) => {
      val record = new Record()

      record.date = firstRecord.date
      record.day = firstRecord.day
      record.month = firstRecord.month
      record.year = firstRecord.year
      record.subMetering1 = firstRecord.subMetering1 + secondRecord.subMetering1
      record.subMetering2 = firstRecord.subMetering2 + secondRecord.subMetering2
      record.subMetering3 = firstRecord.subMetering3 + secondRecord.subMetering3
      record.totalCost = firstRecord.totalCost + secondRecord.totalCost

      record.activePower = firstRecord.activePower + secondRecord.activePower
      record.reactivePower = firstRecord.reactivePower + secondRecord.reactivePower

      record.voltage = (firstRecord.voltage + secondRecord.voltage) / 2
      record.globalIntensity = (firstRecord.globalIntensity + secondRecord.globalIntensity) / 2
      record

    })
    groupRDD


  }

  def monthlyAggregator(inputRDD: RDD[Record]): RDD[((Int, Long), Record)] = {
    val groupRDD = inputRDD.map(record => ((record.month, record.year), record)).reduceByKey((firstRecord, secondRecord) => {
      val record = new Record()

      record.date = firstRecord.date
      record.day = firstRecord.day
      record.month = firstRecord.month
      record.year = firstRecord.year
      record.subMetering1 = firstRecord.subMetering1 + secondRecord.subMetering1
      record.subMetering2 = firstRecord.subMetering2 + secondRecord.subMetering2
      record.subMetering3 = firstRecord.subMetering3 + secondRecord.subMetering3
      record.totalCost = firstRecord.totalCost + secondRecord.totalCost

      record.activePower = firstRecord.activePower + secondRecord.activePower
      record.reactivePower = firstRecord.reactivePower + secondRecord.reactivePower

      record.voltage = (firstRecord.voltage + secondRecord.voltage) / 2
      record.globalIntensity = (firstRecord.globalIntensity + secondRecord.globalIntensity) / 2
      record

    })
    groupRDD


  }
}





