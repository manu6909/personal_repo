package com.mmukundan.twitterproj.udf

import java.io.Serializable
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime,DateTimeZone}

/**
  * Serializable UDF class used by Spark SQL
  *
  */
object UDF extends Serializable {

  /**
    * UDF function for getting string representation of epoch time stamp using year, month, day and hour values only
    *
    */
  val getTimeKey =  udf((epoch: Long) => {
    val in_dateTime = new DateTime(epoch,DateTimeZone.UTC)
    val out_dateTime = new DateTime(in_dateTime.getYear, in_dateTime.getMonthOfYear,in_dateTime.getDayOfMonth,
      in_dateTime.getHourOfDay,0)
    out_dateTime.getMillis.toString
  })

  /**
    * UDF function for identifying the hashtag combinations for a particular tweet
    *
    */
  val getComb = udf((hashtags: Seq[String]) => {
    if (hashtags.length > 1)  {
      hashtags.toArray.sortWith(_ < _).combinations(2).toArray.map(_.mkString("-")).mkString(":")
    }
    else ""
  })
}
