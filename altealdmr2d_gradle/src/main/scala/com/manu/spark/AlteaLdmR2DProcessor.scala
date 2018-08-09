package com.manu.spark

import com.manu.spark.model.Model._
import com.manu.spark.util.SparkUtils
import com.manu.spark.udf.UDF._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Companion object for AlteaLdmR2DProcessor class
  */
object AlteaLdmR2DProcessor {

  /**
    * AlteaLdmR2DProcessor instance creation
    * @param args user parameter instance
    * @return AlteaLdmR2DProcessor instance
    */
  def apply(args: AlteaR2DArgs): AlteaLdmR2DProcessor  = {
    var processor = new AlteaLdmR2DProcessor()
    processor.args = args
    processor
  }
}

/**
  * AlteaLdmR2DProcessor class with methods to
  * read data from raw layer and move to decomposed.
  */
class AlteaLdmR2DProcessor extends SparkUtils {
  private var args: AlteaR2DArgs = _

  /**  Run method which actually initializes spark and does the processing
    *
    *  @param df Dataframe with input data
    *  @return DataFrame Spark dataframe for processed R2D data
    */
  def processData(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.select($"metadata.messageTime".as("tibco_messageTime"),
      getFormattedMessage($"message").as("message_list"))
        .withColumn("tibco_date", to_date(to_utc_timestamp($"tibco_messageTime", "Asia/Dubai")))
        .withColumn("tibco_m", month($"tibco_date"))
        .withColumn("flight_date", when($"tibco_m" >= $"message_list.flight_month".cast("Int"), concat_ws("-",
          date_format($"tibco_date", "yyyy"), $"message_list.flight_month", $"message_list.flight_day")) otherwise
          concat_ws("-", date_format(add_months($"tibco_date", -12), "yyyy"), $"message_list.flight_month", $"message_list.flight_day"))
      .select($"tibco_messageTime", $"message_list.flight_no".as("flight_no"), $"flight_date", $"message_list.aircraft_reg".as("aircraft_reg"),
        $"message_list.aircraft_ver".as("aircraft_ver"), $"message_list.dep_station".as("dep_station"),
        $"message_list.arr_station".as("arr_station"), $"message_list.bag_payload".as("flight_baggage_weight"))
      .withColumn("HELIX_UUID",generateUUID())
      .withColumn("HELIX_TIMESTAMP",generateTimestamp())
  }

  /**
    *  Overwritten method for super class
  */
  override def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String] = Some("overwrite"),
                           coalesceValue: Option[Int] = Some(args.part) ): Unit = super.writeToHDFS(output, hdfsPath,
    outputFormat,saveMode,coalesceValue)
}
