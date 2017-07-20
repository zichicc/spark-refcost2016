package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import org.apache.spark.sql.Dataset
import utils.SparkSessionBuilder
import utils.Resources._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

object Extraction extends SparkSessionBuilder {

  import spark.implicits._

//  private val schema: StructType =
//  ScalaReflection.schemaFor[DataPerComune].dataType.asInstanceOf[StructType]

  //private val schema: StructType =

  private val csvOptions =
    Map("inferSchema" -> "true", "header" -> "true", "delimiter" -> ";")

  def extract(dataFile: String) =
    spark.read
    //.schema(schema)
      .options(csvOptions)
      .csv(resourcePath(dataFile))
//      .select(
//        'DESCREGIONE.ali
//      )
  //.as[DataPerComune]

}
