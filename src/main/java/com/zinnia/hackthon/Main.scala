package com.zinnia.hackthon

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Author: madhu
 */
/**
 * Entry class
 */
object Main {
  def main(args: Array[String]) {
    val inputFile = "src/main/resources/household_power_consumption.txt"
    val results = "results"
    
    val sparkContext = new SparkContext("local", "dataCleaning")
    val inputRawRDD = sparkContext.textFile(inputFile)
    
    /**Clean the data */
    val dataCleaner = new DataCleaner()
    val withoutMissingValuesRDD = dataCleaner.removeMissingValues(inputRawRDD)
    val inputRDD = dataCleaner.convertToFormat(withoutMissingValuesRDD)
    inputRDD.cache()

    val metricsCalculator = new MetricsCalculator(sparkContext)
    val energyConsumptionPrediction = new EnergyUsagePrediction(sparkContext)


    /** Calculate revenue loss whenever there is power outage */
    val revenueLossForOneDay = metricsCalculator.getRevenueLoss(inputRDD)
    sparkContext.makeRDD(List(revenueLossForOneDay)).saveAsTextFile(results + "/RevenueLossForOneDayOutage")

    /** Predict peak time load for weekend and weekday for next one week */
    val(peakTimeLoadWeekday,peakTimeLoadWeekend)=metricsCalculator.getPeekLoad(inputRDD)
    sparkContext.makeRDD(List(peakTimeLoadWeekday)).saveAsTextFile(results + "/PeakTimeLoadWeekday")
    sparkContext.makeRDD(List(peakTimeLoadWeekend)).saveAsTextFile(results + "/PeakTimeLoadWeekend")


    /** predict next day use */
    val  predictedValeKW = energyConsumptionPrediction.getNextDayEnergyConsumption(inputRDD)
    sparkContext.makeRDD(List(predictedValeKW)).saveAsTextFile(results + "/nextdaypowerconsumption")


    /** predict the weekly usage*/
    val predictionArray = energyConsumptionPrediction.getWeeklyEnergyConsumption(inputRDD)
    sparkContext.makeRDD(predictionArray).saveAsTextFile(results+"/weeklyPrediction")


  }


}
