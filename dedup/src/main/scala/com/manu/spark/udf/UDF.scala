package com.manu.spark.udf

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.TimeZone
import scala.util.{Success, Try}
import org.apache.spark.sql.functions.udf

object UDF extends Serializable{

  /**
    * UDF function for getting the arrival station
    *
    */
  val getArrivalStation =  udf((arrivalDepartureFlag: String, viaRoute: String) => arrivalDepartureFlag.toLowerCase match {
    case "a" => "DXB"
    case _   => Try {viaRoute.substring(0,3)} match {
      case Success(succ) => succ
      case _ => null
    }
  })

  /**
    * UDF function for getting the departure station
    *
    */
  val getDepartureStation =  udf((arrivalDepartureFlag: String, viaRoute: String) => arrivalDepartureFlag.toLowerCase match {
    case "d" => "DXB"
    case _   => Try {viaRoute.substring(viaRoute.length-3, viaRoute.length)} match {
      case Success(succ) => succ
      case _ => null
    }
  })

  /**
    * UDF function for formatting the date_time string (change from dd/MM/yyyy HH:mm:ss to yyyy-MM-dd'T'HH:mm)
    *
    */
  val getFormattedDate = udf((date_time: String) => {
    //val tibcotime_rex = """^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.\d+\+\d{2}:\d{2}$""".r
    val histtime_rex = """^(\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}(:\d{2})?)$""".r
    val date_time_updated = if (date_time matches "^\\d{2}\\/\\d{2}\\/\\d{4}$") date_time+" 00:00:00" else date_time
    date_time_updated match {
      case histtime_rex(value1,value2) => {
        val formats = if(value1 matches "^\\d{2}\\/\\d{2}\\/\\d{4} \\d{2}:\\d{2}$")
          (new SimpleDateFormat("dd/MM/yyyy HH:mm"), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")) else
          (new SimpleDateFormat("dd/MM/yyyy HH:mm:ss"), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"))
        val tz = (a:(SimpleDateFormat,SimpleDateFormat)) =>
          (a._1.setTimeZone(TimeZone.getTimeZone("Asia/Dubai")),a._2.setTimeZone(TimeZone.getTimeZone("Asia/Dubai")))
        tz apply formats
        formats._2.format(formats._1.parse(value1))
      }
      //case tibcotime_rex(value) => value
      case _ => null
    }
  })
  
    /**
    * UDF function to calculate total weight for a specific category (multileg flights)
    * @param load_type (baggage_weight, cargo_weight, miscl_weight, mail_weight, transit_weight)
    * @return total weight
    */
   def custom_sum(load_type: String) = udf((rows: Seq[Row], arr_station: String) => {
    var total_load:Long = 0
    val max_leg = rows.map(_.getAs[Int]("flight_leg_number")).max
    rows.count(_.getAs[Int]("flight_leg_number") == max_leg) match {
      case 1 =>             // Last leg
        total_load = rows.filter(_.getAs[String]("off_point") == arr_station).map(_.getAs[String](load_type).toLong).sum
      case _ =>             // All legs except the last leg
        total_load = rows.filter(_.getAs[Int]("flight_leg_number") == max_leg).map(_.getAs[String]("off_point")).map(off_point =>
          rows.filter(_.getAs[String]("off_point") == off_point).map(_.getAs[String](load_type).toLong).sum).sum
    }
    total_load
  })
}
