package com.mmukundan.twitterproj.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray

/**
  * User Defined Aggregate Function class to generate the most common hashtag combinations for a
  * particular hour. For more details refer
  * https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html
  */
class UDAF extends UserDefinedAggregateFunction {

  //Input data schema definition
  override def inputSchema: StructType =
    StructType(StructField("hashtag_comb", StringType) :: Nil)

  //Intermediate data schema definition
  override def bufferSchema: StructType =
    StructType(StructField("buff_data", ArrayType(StringType)) :: Nil)

  //Abstract method called only once per group
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[String]();
  }

  override def deterministic:   Boolean = true;

  // Return data type
  def dataType:DataType = StringType;

  //Abstract method called against every entry in the group
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[WrappedArray[String]](0) :+ input.getAs[String](0)
  }

  //Abstract method to process two intermediate buffers
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[WrappedArray[String]](0) ++ buffer2.getAs[WrappedArray[String]](0)
  }

  //Abstract method called after all entries in a group are exhausted. Final processing done here
  override def evaluate(buffer: Row): Any = {
    val in_array = buffer.getAs[WrappedArray[String]](0).filter(_.nonEmpty);
    var hashtag_array = Array[String]()
    var output_array = Array[String]()
    if(in_array.length > 0) {
      for(iter <- in_array) {
        iter.split(":").foreach(x => (hashtag_array = hashtag_array :+ x))
      }
      val ranked_map: Map[String,Int] = hashtag_array.groupBy(identity).mapValues(_.size)
      val top_rank = ranked_map.maxBy(_._2)._2
      val filtered_map = ranked_map.filter((t) => t._2 == top_rank)
      filtered_map.foreach((t) => (output_array = output_array :+ (t._1+" : "+t._2.toString)))
      output_array.mkString(" , ")
    }
  }
}
