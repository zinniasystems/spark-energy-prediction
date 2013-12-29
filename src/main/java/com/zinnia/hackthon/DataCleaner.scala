package com.zinnia.hackthon

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.text.SimpleDateFormat

/**
 * Author: madhu
 */
/**
 * Class to clean the data
 */
class DataCleaner {
  /**
   *
    * @param inputRDD
   * @return removes the missing data
   */
   def removeMissingValues(inputRDD:RDD[String]):RDD[String]={
      inputRDD.filter(line=> !(line.contains("?") || (line.contains("Global_active_power;Global_reactive_power"))))
   }
   /**Converting to internal format */

   def convertToFormat(inputRDD:RDD[String]):RDD[Record] ={
     val fullFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
     val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
     inputRDD.map(line => {
       val values = line.split(";")
       val record = new Record()
       val date = fullFormat.parse(values{0}+" "+values{1})
       record.id=date.getTime
       val dateValue = dateFormat.parse(values{0})
       val dateString=dateFormat.format(dateValue)
       record.date = dateString
       record.time = values{1}
       record.day = date.getDay
       record.month = date.getMonth
       record.year = date.getYear
       record.hourofDay= date.getHours
       record.activePower = values{2}.toDouble
       record.reactivePower = values{3}.toDouble
       record.voltage = values{4}.toDouble
       record.globalIntensity= values{5}.toDouble
       record.subMetering1=values{6}.toDouble
       record.subMetering2= values{7}.toDouble
       record.subMetering3 = values{8}.toDouble
       record
     })
   }
}





