package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils.{DataPerComune, SparkSessionBuilder}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{trim, upper}
import org.apache.spark.storage.StorageLevel

object Extract extends SparkSessionBuilder {

  import spark.implicits._

  private lazy val italianRegions: Seq[String] = List(
    "ABRUZZO",
    "BASILICATA",
    "CALABRIA",
    "CAMPANIA",
    "EMILIA-ROMAGNA",
    "FRIULI-VENEZIA GIULIA",
    "LAZIO",
    "LIGURIA",
    "LOMBARDIA",
    "MARCHE",
    "MOLISE",
    "PIEMONTE",
    "PUGLIA",
    "SARDEGNA",
    "SICILIA",
    "TOSCANA",
    "TRENTINO-ALTO ADIGE",
    "UMBRIA",
    "VALLE D'AOSTA",
    "VENETO"
  )

  private lazy val schema =
    ScalaReflection.schemaFor[DataPerComune].dataType.asInstanceOf[StructType]

  private lazy val csvOptions =
    Map("header" -> "true",
        "delimiter" -> ";",
        "parserLib" -> "UNIVOCITY",
        "mode" -> "DROPMALFORMED")

  private def process(df: DataFrame): DataFrame = {
    val preProcessedDF: DataFrame =
      df.na.drop // drop rows with null values (that did not conform to the schema)
        .withColumn("regione", upper(trim('regione)))
        .withColumn("provincia", upper(trim('provincia)))
        .withColumn("comune", upper(trim('comune)))
        .distinct // avoid duplicate rows
        .where('regione
          .isin(italianRegions: _*) and 'elettori >= 'elettori_m and 'votanti >= 'votanti_m and
          'votanti === 'voti_si + 'voti_no + 'voti_bianchi + 'voti_nonvalidi + 'voti_contestati)
        .persist(StorageLevel.MEMORY_ONLY_SER)

    val duplicateCheck: DataFrame =
      preProcessedDF
        .groupBy('regione, 'provincia, 'comune)
        .count
    val duplicateCheckColumns: Seq[String] =
      List("regione", "provincia", "comune")

    val postProcessedDF: DataFrame = preProcessedDF
      .join(duplicateCheck, duplicateCheckColumns)
      .where('count === 1) // avoid conflicts if comune inserted twice with different data
      .drop('count)

    postProcessedDF
  }

  def extract(filePath: String): Dataset[DataPerComune] = {
    val df: DataFrame = spark.read
      .schema(schema)
      .options(csvOptions)
      .csv(filePath)
    val processedDF: Dataset[DataPerComune] = process(df).as[DataPerComune]

    processedDF
  }
}
