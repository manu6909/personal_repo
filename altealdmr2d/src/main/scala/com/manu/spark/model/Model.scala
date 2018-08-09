package com.manu.spark.model

/**
  * Model class with different case classes for supporting operations
  */

object Model {
  case class AlteaR2DArgs(in_path: String = null,out_path: String = null,compression: String = null,
                  in_format: String = null, out_format: String = null, part: Int = 2)
}
