package com.manu.spark.udf

import java.io.Serializable
import org.apache.spark.sql.functions.udf
import java.util.UUID
import util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

/**
  * UDF class
  */
object UDF extends Serializable{

  case class bag_payload(var baggage_weight: String = null, var cargo_weight: String = null, var mail_weight: String = null,
                                       var transit_weight: String = null, var miscl_weight: String = null, var off_point: String = null,
                                       var tail_order: String = null)
  case class ldm_message (var flight_no: String = null,
                          var flight_day: String = null,
                          var flight_month: String = null,
                          var aircraft_reg: String = null,
                          var aircraft_ver: String = null,
                          var dep_station: String = null,
                          var arr_station: String = null,
                          var bag_payload: Array[bag_payload] = null)

  lazy val ldm_rex_flight1 = """^\w+\/(\d{2})\.(\S+)\.(.+)\.\d+\/(\d+|\d+\/\d+)$""".r
  lazy val ldm_rex_flight2 = """^(\S+)\/\d{2}(\D{3})\/(\S{3})$""".r
  lazy val ldm_rex_offpoint = """^-(\S{3})\..*$""".r
  lazy val ldm_rex_weight = """^(\S{3})\sC\s*(\d+)\sM\s*(\d+)\sB.+\/\s*(\d+)\sO\s*(\d+)\sT\s*(\d+)$""".r

  /**
    * UDF function to get altea ldm leg level details for specific flight
    *
    */
  val getFormattedMessage =  udf((message: String) => {
    val mnth_map = Map[String,String]("JAN" -> "01", "FEB" -> "02", "MAR" -> "03", "APR" -> "04", "MAY" -> "05", "JUN" -> "06", "JUL" -> "07",
    "AUG" -> "08", "SEP" -> "09", "OCT" -> "10", "NOV" -> "11", "DEC" -> "12")
    var _bag_payload = ArrayBuffer.empty[bag_payload]
    var tail_order = 1
    val _ldm_message = ldm_message()
    val entries: Array[String] = message.split("\\R")
    try {
      for(entry <- entries) {
        entry.trim match {
          case ldm_rex_flight1(flight_day, aircraft_reg, aircraft_ver, dummy) => {
            _ldm_message.flight_day = flight_day
            _ldm_message.aircraft_reg = aircraft_reg
            _ldm_message.aircraft_ver = aircraft_ver
          }
          case ldm_rex_flight2(flight_no, flight_month, dep_station) => {
            _ldm_message.flight_no = flight_no
            _ldm_message.flight_month = mnth_map(flight_month)
            _ldm_message.dep_station = dep_station
          }
          case ldm_rex_offpoint(off_point) =>{
            _bag_payload = _bag_payload += bag_payload(tail_order = tail_order.toString, off_point = off_point)
            if (1 == tail_order) {
              _ldm_message.arr_station = off_point
            }
            tail_order = tail_order + 1
          }
          case ldm_rex_weight(off_point, c, m, b, o, t) => {
            var index = 1
            breakable{
              for(payload <- _bag_payload) {
                if (off_point == payload.off_point) {
                  index = _bag_payload.indexOf(payload)
                  break()
                }
              }
            }
            _bag_payload(index).cargo_weight = c
            _bag_payload(index).mail_weight = m
            _bag_payload(index).baggage_weight = b
            _bag_payload(index).miscl_weight = o
            _bag_payload(index).transit_weight = t
          }
          case _ =>
        }
      }
      _ldm_message.bag_payload = _bag_payload.toArray
    }
    catch {
      case e: Exception => _ldm_message.bag_payload = Array(null.asInstanceOf[bag_payload])
    }
    _ldm_message
  })

  /**
    * UDF function for generating unique ID
    *
    */
  val generateUUID =  udf(() => UUID.randomUUID().toString)

  /**
    * UDF function for generating timestamp in seconds
    *
    */
  val generateTimestamp =  udf(() => (System.currentTimeMillis()/1000L).toString)
}
