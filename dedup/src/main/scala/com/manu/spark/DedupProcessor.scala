package com.manu.spark

import com.manu.spark.model.Model._
import com.manu.spark.util.SparkUtils
import com.manu.spark.udf.UDF._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window

/**
  * Companion object for DedupProcessor class
  */
object DedupProcessor {
  /**
    * DedupProcessor instance creation
    * @param args user parameter instance
    * @return DedupProcessor instance
    */
  def apply(args: Args): DedupProcessor = {
    var processor = new DedupProcessor()
    processor.args = args
    processor
  }
}
/**
  * DedupProcessor class with methods to
  * read data, perform dedup and write to hdfs.
  */
class DedupProcessor extends SparkUtils {
  private var args: Args = _

  /**
    * Method to process the data
    * @param incr_df
    * @param hist_df
    * @param hiveContext
    * @return Dataframe
    */
  def processData(incr_df: DataFrame, hist_df: DataFrame)(implicit hiveContext: HiveContext): (DataFrame,Option[DataFrame]) = {
    import hiveContext.implicits._

    val (hist_df_updated,incr_df_updated) = args.init match {                     // Logic to get arrival and departure station from historic data
      case true => {
        (hist_df.withColumn("arrivalStation", getArrivalStation($"arrivalDepartureFlag",$"route"))
          .withColumn("departureStation",getDepartureStation($"arrivalDepartureFlag",$"route"))
          .withColumn("bchtStaStd", getFormattedDate($"bchtStaStd"))
          .withColumn("tibco_messageTime", getFormattedDate($"tibco_messageTime"))
          .withColumn("srcCreateDate", $"tibco_messageTime"),
          incr_df.withColumn("arrivalStation", getArrivalStation($"arrivalDepartureFlag",$"route"))
            .withColumn("departureStation",getDepartureStation($"arrivalDepartureFlag",$"route"))
            .withColumn("tibco_messageTime", date_format($"tibco_messageTime","yyyy-MM-dd'T'HH:mm:ss.SSS")))

      }
      case false => (hist_df, incr_df.withColumn("arrivalStation", getArrivalStation($"arrivalDepartureFlag",$"route"))
        .withColumn("departureStation",getDepartureStation($"arrivalDepartureFlag",$"route"))
        .withColumn("tibco_messageTime", date_format($"tibco_messageTime","yyyy-MM-dd'T'HH:mm:ss.SSS")))
    }

    val cols = incr_df_updated.columns.toSet.intersect(hist_df_updated.columns.toSet).map(col).toSeq
    val df = incr_df_updated.select(cols: _*).unionAll(hist_df_updated.select(cols: _*))
    var current_data: DataFrame = null
    var hist_data: Option[DataFrame] = None

    val window = Window.partitionBy($"airlineCode", $"bchtFlightNumber", $"bchtStaStd", $"arrivalDepartureFlag", $"aircraftRegistrationNumber",
      $"bchtOldBayNo",  $"bchtNewBayNo")
    val input = df.select($"airlineCode", $"bchtFlightNumber", $"bchtStaStd", $"arrivalDepartureFlag",
      $"aircraftRegistrationNumber", $"departureStation", $"arrivalStation", $"bchtReasonId", $"bchtOldBayNo", $"bchtNewBayNo",
      $"tibco_messageTime", $"srcCreateDate")
      .where(lower($"airlineCode") === "ek")
    val clean_records = input.where($"airlineCode".isNotNull && $"bchtFlightNumber".isNotNull &&
      $"aircraftRegistrationNumber".isNotNull && $"bchtStaStd".isNotNull)
      .distinct()
      .withColumn("max_srctime",max($"srcCreateDate").over(window))
      .where($"max_srctime" === $"srcCreateDate")
      .withColumn("max_tibcotime", max($"tibco_messageTime").over(window))
      .where($"max_tibcotime" === $"tibco_messageTime")

    val max_date = clean_records
      .select(max(to_date(to_utc_timestamp($"bchtStaStd".cast(TimestampType),"Asia/Dubai")))).first().getDate(0)

    val final_clean_records = clean_records
      .select($"airlineCode", $"bchtFlightNumber", $"bchtStaStd", $"arrivalDepartureFlag",
        $"aircraftRegistrationNumber", $"departureStation", $"arrivalStation", $"bchtReasonId", $"bchtOldBayNo",
        $"bchtNewBayNo", $"tibco_messageTime", $"srcCreateDate", lit(max_date).alias("max_flightdate"))
      .cache()

    val rejected_records = input.where($"airlineCode".isNull || $"bchtFlightNumber".isNull || $"aircraftRegistrationNumber".isNull
      || $"bchtStaStd".isNull )

    args.init match {
      case true => {        // Logic for first day load. Data will be splitted to history and current based on months parameter
        current_data = final_clean_records.where(to_date(to_utc_timestamp($"bchtStaStd","Asia/Dubai")) >=
          add_months($"max_flightdate", (-1 * args.months))).drop($"max_flightdate")
        hist_data = Some(final_clean_records.where(to_date(to_utc_timestamp($"bchtStaStd","Asia/Dubai")) <
          add_months($"max_flightdate", (-1 * args.months))).drop($"max_flightdate"))
      }
      case false => current_data = final_clean_records.drop($"max_flightdate")
    }
    (current_data,hist_data)
  }

  /**  Overwritten method for super class
    */
  override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("overwrite"),
                           coalesceValue: Option[Int] = Some(args.part) ): Unit = super.writeToHDFS(output, hdfsPath,
    outputFormat,saveMode,coalesceValue)
}
