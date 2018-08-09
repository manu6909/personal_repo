package com.manu.spark

import com.manu.spark.model._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
  * DedupDriver class with main method for starting point for the app
  */
object DedupDriver {
  private val compress_rex = """^snappy$|^deflate$""".r
  private val format_rex = """^avro$|^parquet$""".r

  private lazy val logger:Logger = Logger.getLogger(DedupDriver.getClass)

  /** Main method
    *
    *  @param args Command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Model.DMISBayDetailsArgs]("spark-submit <spark-options> --class com.manu.spark.DmisBayHistoryDedupDriver" +
      " dmis-bayhistory-dedup-<jar version>.jar") {
      head("DMISBayHistoryDedup")
      opt[String]('i', "incr_path")
        .required()
        .action((x, config) => config.copy(incr_path = x))
        .text("Required parameter : Incremental data input path")
      opt[String]('h', "hist_path")
        .required()
        .action((x, config) => config.copy(hist_path = x))
        .text("Required parameter : Input path for historic data (History data on day 1 and deduped data from day 2 onwards")
      opt[String]('o', "h_out_path")
        .optional()
        .action((x, config) => config.copy(h_out_path = Some(x)))
        .text("Optional parameter : Output file path for history data store needed for initial run only")
      opt[String]('l', "l_out_path")
        .required()
        .action((x, config) => config.copy(l_out_path = x))
        .text("Required parameter : Output file path for latest/current data store")
      opt[Int]('m', "months")
        .optional()
        .action((x, config) => config.copy(months = x))
        .text("Optional parameter : History and current data split month value for initial run. Default : 12 months")
      opt[String]('c', "compression")
        .required()
        .valueName("snappy OR deflate")
        .validate(x => {
          if (!(x.matches(compress_rex.toString()))) Left("Invalid compression specified : " + x) else Right(true)
        } match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action((x, config) => config.copy(compression = x))
        .text("Required parameter : Output data compression format, snappy or deflate")
      opt[Int]('p', "part")
        .required()
        .action((x, config) => config.copy(part = x))
        .text("Required parameter : Number of partitions for coalesce")
      opt[Unit]('t', "init") action {
        (x,conf) => conf.copy(init = true)
      } text("Use this option if it is the first time to load both history and incremental")
      help("help") text ("Print this usage message")
    }
    parser.parse(args, Model.DMISBayDetailsArgs()) map { config =>
      val processor = DmisBayDetailsDedupProcessor(config)
      implicit lazy val sc = new SparkContext(processor.sparkConfig())
      implicit lazy val hiveContext = processor.getHiveContext(sc,config.compression)

      (config.init, config.h_out_path) match {
        case (true, None) => {
          logger.error("Output path to store history data must be provided for initial run. Killing application")
          System.exit(1)
        }
        case (true,Some(_)) => {
          val (cur, hist) = processor.processData(processor.readValueFromHDFS(config.incr_path,"avro"),
            processor.readValueFromHDFS(config.hist_path,"avro"))
          processor.writeToHDFS(cur,config.l_out_path,"parquet")
          processor.writeToHDFS(hist.getOrElse(null), config.h_out_path.getOrElse(""),"parquet")
        }
        case (false,_) => {
          val cur = processor.processData(processor.readValueFromHDFS(config.incr_path,"avro"),
            processor.readValueFromHDFS(config.hist_path,"parquet"))._1
          processor.writeToHDFS(cur,config.l_out_path,"parquet")
        }
      }
    } getOrElse {
      logger.error("Missing input arguments. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
