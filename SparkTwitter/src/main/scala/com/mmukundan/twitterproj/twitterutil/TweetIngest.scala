package com.mmukundan.twitterproj.twitterutil

import java.io.FileWriter
import twitter4j.{Query, TwitterFactory}
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
import net.liftweb.json.JsonDSL._
import net.liftweb.json._
import scala.collection.mutable.ListBuffer
import com.mmukundan.twitterproj.model.Model.Tweet


/**
  * Twitter data ingest singleton object
  *
  */
object TweetIngest {

  private val log: Logger = Logger.getLogger(TweetIngest.getClass)
  private val conf = ConfigFactory.load()
  private val TPP = 100

  /** Method to connect and perform twitter search to get happy hour tweet messages
    *
    *  @param start start date
    *  @param end  end date
    *  @return Unit
    */
  def processTweets(start: String, end:String): Unit = {
   val query_string = ":) since:"+start+" :until:"+end
    val cb: ConfigurationBuilder = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(conf.getString("auth.consumer_key"))
      .setOAuthConsumerSecret(conf.getString("auth.consumer_secret"))
      .setOAuthAccessToken(conf.getString("auth.access_token"))
      .setOAuthAccessTokenSecret(conf.getString("auth.access_token_secret"))

    val tf = new TwitterFactory(cb.build)
    val twitter = tf.getInstance
    val all_tweets = ListBuffer[twitter4j.Status]()
    var since_id = 0L
    var (increment,first_batch_flag) = (0L, false)
    val fw = new FileWriter(conf.getString("out.tweet_file"),true)        //File to save filtered tweets
    try {
      val query = new Query(query_string)
      query.setCount(TPP)                     //Set tweets per page
      val first_tweets = twitter.search(query)
      while(all_tweets.size < conf.getLong("out.max_tweets") && increment < conf.getLong("out.max_tweets")) {
        if(false == first_batch_flag) {
          all_tweets ++= first_tweets.getTweets
          since_id = first_tweets.getTweets.minBy(_.getId).getId
          first_batch_flag = true
        }
        else {
          query.setMaxId(since_id-1)
          val next_tweets = twitter.search(query)
          all_tweets ++= next_tweets.getTweets
          since_id = next_tweets.getTweets.minBy(_.getId).getId
        }
        increment += TPP
      }
      all_tweets.map{x =>
        log.debug("Hashtag count "+ x.getHashtagEntities.size)
        val json_string = createJson(x)
        fw.write(json_string)
      }
      log.info("Finished writing tweets to output file : "+ conf.getString("out.tweet_file"))
    }
    catch {
      case e: Throwable => log.error("Exception while extracting and saving twitter json data "+e.getMessage)
    }
    finally {
      fw.close()
    }
  }

  /** Method to connect and perform twitter search to get happy hour tweet messages
    *
    *  @param status Twitter api response status
    *  @return String representation of tweet message with handpicked fields only
    */
  private def createJson(status: twitter4j.Status): String = {
    var hashtags = Array[String]()
    status.getHashtagEntities.foreach(entry => (hashtags = hashtags :+ entry.getText))
    val tweet = Tweet(status.getId,hashtags.toList,status.getCreatedAt.getTime)
    val json = (
      ("id" -> tweet.id) ~
      ("hashtags" -> tweet.hashtags) ~
      ("timestamp" -> tweet.timestamp)
      )
    compact(render(json))+"\n"
  }

}
