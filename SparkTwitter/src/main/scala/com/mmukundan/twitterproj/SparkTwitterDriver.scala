package com.mmukundan.twitterproj

import org.joda.time.format.DateTimeFormat
import org.apache.log4j.Logger
import com.mmukundan.twitterproj.twitterutil.TweetIngest
import com.mmukundan.twitterproj.model.Model.Config
import com.mmukundan.twitterproj.model.Model.Config
import com.mmukundan.twitterproj.twitterutil.TweetIngest

/**
  * Spark application driver singleton object. Execution starts from main function here
  *
  */
object SparkTwitterDriver {

  private val DATE_FORMAT: String = "yyyy-MM-dd"
  private val logger:Logger = Logger.getLogger(SparkTwitterDriver.getClass)
  private val date_rex = """^\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$""".r
  private val dtf = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC()

  /** Checks the validity of date parameters passed as command line arguments
    *
    *  @param input date range string
    *  @return Either success or error message
    */
  private def checkParams(input: String): Either[String, Boolean] = {
    val date_range = input.split(":")
    val (start_date,end_date) = (dtf.parseDateTime(date_range.head),dtf.parseDateTime(date_range.last))
    if (!(input.matches(date_rex.toString()))) Left("Input dates format error "+ input)
    else if (end_date.isBefore(start_date) || end_date == start_date) Left("End date must be greater than start date")
    else Right(true)
  }

  /** Main method
    *
    *  @param args command line arguments
    *  @return Unit
    */
  def main(args:Array[String]) : Unit = {

    val parser = new scopt.OptionParser[Config]("TwitterHappiestHour") {  //Scopt package for handling commandline inputs nicely
      head("TwitterHappiestHour")
      opt[String]('d', "date")
        .required()
        .valueName(s"<$DATE_FORMAT>")
        .validate(x => checkParams(x) match {
          case Left(l) => failure(l)
          case Right(r) => success
        })
        .action( (x, config) => config.copy(date_range = x) )
        .text(s"Required: daterange to process as $DATE_FORMAT:$DATE_FORMAT")

      opt[Unit]('r', "dryrun") action {           //Currently this has no effect
        (x,conf) => conf.copy(dryrun = true)
      } text("Dryrun mode : Samples the data and prints it to console")

      help("help") text("Print this usage message")
    }

    parser.parse(args, Config()) map { config =>
      val date_range = config.date_range.split(":")
      val (start,end) = (date_range.head,date_range.last)
      logger.info("Tweets start date : "+start)
      logger.info("Tweets end date : "+end)
      TweetIngest.processTweets(start,end)         //Stage 1: Filter tweets and write to file
      SparkTwitterProcessor.run(config)            //Stage 2 : Find out happty hour
    } getOrElse {
      logger.error("CLI argument error. Unable to submit spark application")
      println
      parser.showUsageAsError
      println
    }
  }
}
