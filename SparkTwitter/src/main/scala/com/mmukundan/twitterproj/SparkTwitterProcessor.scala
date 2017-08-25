package com.mmukundan.twitterproj

import org.apache.log4j.Logger
import com.mmukundan.twitterproj.model.Model.Config
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import com.mmukundan.twitterproj.udf.UDF
import com.mmukundan.twitterproj.udaf.UDAF
import com.mmukundan.twitterproj.udf.UDF
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, DateTimeZone}


/**
  * Spark application processor singleton object. Data aggregation and processing done here using Spark SQL
  *
  */
object SparkTwitterProcessor {
  private val log: Logger = Logger.getLogger(SparkTwitterProcessor.getClass)
  private val conf = ConfigFactory.load()

  /** Run method which will be performing data processing based on Spark SQL
    *
    *  @param config config paramters
    *  @return Unit
    */
  def run(config: Config): Unit = {
    log.info("Started processing twitter data to identify happy hour")
    val spark = SparkSession.builder().appName("SparkTwitterHappiestHour").getOrCreate()
    import spark.implicits._;
    spark.udf.register("cust_agg", new UDAF)
    val cust_agg = new UDAF;
    if(Files.exists(Paths.get("file://"+conf.getString("out.tweet_file")))) {
      log.error("Input file with tweets not found error. Exiting the application")
      spark.stop()
      sys.exit(1)
    }
    val data = spark.read.option("mode","DROPMALFORMED")
      .option("header",false)
      .json(conf.getString("out.tweet_file"))
      .withColumn("datetime_key",UDF.getTimeKey($"timestamp"))
      .repartition($"datetime_key")                         //Partitioning done based on datetime for parallelism
      .withColumn("hashtag_comb",UDF.getComb($"hashtags"))
      .groupBy($"datetime_key")
      .agg(first($"datetime_key").as("timestamp"),
        count($"id").as("tweet_count"),cust_agg($"hashtag_comb").as("top_comb"))
    val happy_hour = data.agg(max($"tweet_count").as("max_count"))

    log.info("happiest hour")

    val output = data.join(happy_hour,$"tweet_count" === $"max_count")
      .select(concat(lit("["),$"timestamp",lit("]"),lit(", most common combination of hashtags this hour : "),
    $"top_comb").as("output")).map(row => row.getAs[String](0))
    output.collect.foreach(println)
    spark.stop()
    log.info("Finished application execution")
  }
}
