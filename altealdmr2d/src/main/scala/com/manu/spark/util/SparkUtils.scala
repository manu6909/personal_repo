package com.manu.spark.util

import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait SparkUtils {

  /**
    * Reads data from HDFS
    *
    *  @param spark        sparkSession object
    *  @param hdfsPath     The path on HDFS where the results are to be stored
    *  @param inputFormat  The underlying file format. Currently Avro and Parquet are supported
    *  @return DataFrame object
    *
    */
  def readValueFromHDFS(hdfsPath: String, inputFormat: String, spark: SparkSession): DataFrame = {

    import com.databricks.spark.avro._
    import org.apache.spark.sql.DataFrameReader
    import spark.implicits._

    val dfReader:DataFrameReader = spark.read

    inputFormat.toLowerCase() match {
      case "parquet" ⇒ {
        Try { dfReader.parquet(hdfsPath) } match {
          case Failure(df)   ⇒ { throw new Exception(df.getCause) }
          case Success(succ) ⇒ succ
        }
      }
      case "avro" ⇒ {
        Try { dfReader.avro(hdfsPath) } match {
          case Failure(df)   ⇒ { throw new Exception(df.getCause) }
          case Success(succ) ⇒ succ
        }
      }
      case "sequence" => {
        Try {spark.sparkContext.sequenceFile[LongWritable, BytesWritable](hdfsPath)
          .map(x => new String(x._2.copyBytes(), "utf-8"))} match {
          case Failure(df)   ⇒ { throw new Exception(df.getCause) }
          case Success(succ) ⇒ spark.read.json(succ.toDS())
        }
      }
      case _ ⇒ throw new UnsupportedOperationException(s"The output format $inputFormat is not supported")
    }
  }

  /**
    * Writes out the result to HDFS
    *
    *  @param output  DataFrame:      The result of a computation
    *  @param hdfsPath      The path on HDFS where the results are to be stored
    *  @param outputFormat  The underlying file format. Currently Avro and Parquet are supported
    *  @param saveMode      The Save mode append or overwrite
    *  @param coalesceValue The coalesce value to use
    */
  def writeToHDFS(output: DataFrame, hdfsPath: String, outputFormat: String, saveMode: Option[String], coalesceValue: Option[Int]): Unit = {

    import com.databricks.spark.avro._
    import org.apache.spark.sql.DataFrameWriter
    val mode = saveMode.getOrElse("overwrite")
    var dfWriter: Option[DataFrameWriter[Row]] = None

    if (coalesceValue.isEmpty) {
      dfWriter = Some(output.write.mode(mode))
    } else {
      dfWriter = Some(output.coalesce(coalesceValue.get).write.mode(mode))
    }
    val result = dfWriter.getOrElse(throw new RuntimeException("Something went wrong while trying to coalesce final result"))
    outputFormat.toLowerCase() match {
      case "parquet" ⇒ result.parquet(hdfsPath)
      case "avro"    ⇒ result.avro(hdfsPath)
      case _         ⇒ throw new UnsupportedOperationException(s"The output format $outputFormat is not supported")
    }
  }


  /**
    * Get spark session with the config parameters
    * @param compression output compression
    * @return spark session object
    */
  def getSparkSession(compression: String, appName: String = ""): SparkSession = {
    SparkSession.builder()
      .config("parquet.filter.statistics.enabled", "true") // Turn on parquet filter push-down,stats filtering, and dictionary filtering
      .config("parquet.filter.dictionary.enabled", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.hive.convertMetastoreParquet", "true") // Use non-hive read path
      .config("spark.sql.parquet.mergeSchema", "false")  // Turn off schema merging
      .config("spark.sql.hive.convertMetastoreParquet.mergeSchema", "false")
      .config("spark.sql.parquet.compression.codec", compression) // Set parquet compression
      .config("spark.sql.avro.compression.codec", compression) // Configure avro parameters
      .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.dynamicAllocation.enabled","true")
      .config("spark.shuffle.service.enabled","true")
      .config("spark.rdd.compress", "true")
      .config("spark.core.connection.ack.wait.timeout", "600")
      //.set("spark.akka.frameSize", "512")
      //.set("spark.akka.threads", "10")
      //.set("spark.eventLog.enabled", "true")
      .config("spark.io.compression.codec", "lzf")
      .config("spark.speculation", "true")
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
  }
}
