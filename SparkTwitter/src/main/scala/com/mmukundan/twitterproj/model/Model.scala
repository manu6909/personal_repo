package com.mmukundan.twitterproj.model

/**
  * Singleton class for keeping all case classes
  *
  */
object Model {
  case class Tweet(id: Long, hashtags: List[String],timestamp: Long)
  case class Config(date_range: String = null, dryrun: Boolean = false)
}
