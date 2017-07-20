package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils.SparkSessionBuilder
import utils.Resources._

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.trim

object Extraction extends SparkSessionBuilder {

  import spark.implicits._

  private val schema =
    ScalaReflection.schemaFor[DataPerComune].dataType.asInstanceOf[StructType]

  private val csvOptions =
    Map("header" -> "true", "delimiter" -> ";", "mode" -> "DROPMALFORMED")

  private def preProcess(df: DataFrame): DataFrame = {
    df.na.drop
      .where('elettori >= 'elettori_m and 'votanti >= 'votanti_m and
        'votanti === 'voti_si + 'voti_no + 'voti_bianchi + 'voti_nonvalidi + 'voti_contestati)
  }

  private def postProcess(df: DataFrame): Dataset[DataPerComune] =
    df.withColumn("regione", trim('regione))
      .withColumn("provincia", trim('provincia))
      .withColumn("comune", trim('comune))
      .dropDuplicates("regione", "provincia", "comune")
      .as[DataPerComune]

  def extract(fileName: String): Dataset[DataPerComune] = {

    val df: DataFrame = spark.read
      .schema(schema)
      .options(csvOptions)
      .csv(resourcePath(fileName))

    val integrityChecked: DataFrame = preProcess(df)

    postProcess(integrityChecked)
  }

}
