package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import org.apache.spark.sql.Dataset
import utils.SparkSessionBuilder
import utils.Resources._
import refcost2016.Models._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

object Extraction extends SparkSessionBuilder {

  import spark.implicits._

  val schema: StructType =
    ScalaReflection.schemaFor[DataPerComune].dataType.asInstanceOf[StructType]

  val csvOptions =
    Map("header" -> "true", "delimiter" -> ";")

  // match against positive integer numbers (excluding leading zeros, including 0)
  val numRegex: Regex = """^([1-9]\d*|0)$""".r

  def extract(dataFile: String): Dataset[DataPerComune] =
    spark.read
      .schema(schema)
      .options(csvOptions)
      .csv(resourcePath(dataFile))
      .as[DataPerComune]

}
