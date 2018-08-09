package com.manu.spark.model

/**
  * Model class with different case classes for supporting operations
  */

object Model {

  case class Args(incr_path: String = null, hist_path: String= null, h_out_path: Option[String] = None, l_out_path: String = null,
                           compression: String = null, init: Boolean = false, months: Int = 12, part: Int = 0)
}
