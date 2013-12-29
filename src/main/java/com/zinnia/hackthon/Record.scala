package com.zinnia.hackthon

/**
 * Author: madhu
 * Internal representation of record
 */
class Record() extends  Serializable {

  var id: Long = _
  var date : String = _
  var time : String = _
  var day: Int = _
  var hourofDay: Long = _
  var month: Int = _
  var year: Long = _
  var activePower: Double = _
  var reactivePower: Double = _
  var voltage: Double = _
  var globalIntensity: Double = _
  var subMetering1: Double = _
  var subMetering2: Double = _
  var subMetering3: Double = _
  var totalCost: Double = _
  def totalPowerUsed: Double = activePower * 1000 / 60
  def powerMetered: Double = subMetering1 + subMetering2 + subMetering3

  override def toString: String = {
    id + ";" + day.toString + ";" + hourofDay + ";" + month +
      ";" + year + ";" + activePower + ";" + reactivePower + ";" +
      voltage + ";" + globalIntensity + ";" + subMetering1 + ";" + subMetering2 + ";" + subMetering3 + ";" + totalPowerUsed + ";" + powerMetered
  }

  override def equals(obj: scala.Any): Boolean = this.id == obj.asInstanceOf[Record].id
}
