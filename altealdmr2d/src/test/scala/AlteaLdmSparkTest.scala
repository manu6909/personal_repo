import org.scalatest.{BeforeAndAfter, FunSuite}
import com.holdenkarau.spark.testing.SharedSparkContext
import com.manu.spark.model.Model._
import com.manu.spark.AlteaLdmR2DProcessor
import org.apache.spark.sql.functions.{asc,desc}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith

/**
  * Test class file
  */
@RunWith(classOf[org.scalatest.junit.JUnitRunner])
class AlteaLdmSparkTest extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var processor: AlteaLdmR2DProcessor = _
  var in_df: DataFrame = _
  implicit lazy val _spark : SparkSession =  SparkSession.builder().appName("AlteaLdmR2DTest").getOrCreate()
  import _spark.implicits._
  override implicit def reuseContextIfPossible: Boolean = true

  /**
    * Preparing the tests
    */
  before {
    processor = AlteaLdmR2DProcessor(AlteaR2DArgs())
    in_df = _spark.read.json(_spark.sparkContext.textFile("./src/test/resources/part-r-00000").toDS)
  }

  /**
    * Test case 1 : Compare the raw data and processed data count
    */
  test("Test1 : Compare input and output row count"){
    val actual: Long = processor.processData(in_df).count()
    assert(actual === 24L)
  }

  /**
    * Test case 2 : Check uniqueness of helix UUID
    */
  test("Test2 : Check uniqueness of helix UUID"){
    val actual: Long = processor.processData(in_df).select($"HELIX_UUID").distinct().count()
    assert(actual === 24L)
  }

  /**
    * Test case 3 : Validate TIBCO timestamp available in processed data
    */
  test("Test3 : Check for correct TIBCO timestamp value"){
    val actual: String = processor.processData(in_df).select($"tibco_messageTime").first().get(0).toString
    assert(actual === "2018-04-18T11:02:53.267+04:00")
  }

  /**
    * Test case 4 : Validate Aircraft registration number available in processed data
    */
  test("Test4 : Check for correct aircraft registration number"){
    val actual: String = processor.processData(in_df).select($"aircraft_reg").take(5).last.get(0).toString
    assert(actual === "A6EEX")
  }
}
