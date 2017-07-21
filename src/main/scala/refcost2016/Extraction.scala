package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils.SparkSessionBuilder
import utils.Resources.resourcePath
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{trim, upper}

object Extraction extends SparkSessionBuilder {

  import spark.implicits._

  private val italianRegions: Seq[String] = List(
    "LOMBARDIA",
    "ABRUZZO",
    "LIGURIA",
    "SARDEGNA",
    "TRENTINO-ALTO ADIGE",
    "CAMPANIA",
    "EMILIA-ROMAGNA",
    "MOLISE",
    "SICILIA",
    "BASILICATA",
    "MARCHE",
    "PIEMONTE",
    "VENETO",
    "UMBRIA",
    "FRIULI-VENEZIA GIULIA",
    "LAZIO",
    "VALLE D'AOSTA",
    "PUGLIA",
    "CALABRIA",
    "TOSCANA"
  )

  private val schema =
    ScalaReflection.schemaFor[DataPerComune].dataType.asInstanceOf[StructType]

  private val csvOptions =
    Map("header" -> "true", "delimiter" -> ";", "mode" -> "DROPMALFORMED")

  private def process(df: DataFrame): DataFrame = {
    df.na.drop
      .withColumn("regione", upper(trim('regione)))
      .withColumn("provincia", upper(trim('provincia)))
      .withColumn("comune", upper(trim('comune)))
      .distinct
      .where('regione
        .isin(italianRegions: _*) and 'elettori >= 'elettori_m and 'votanti >= 'votanti_m and
        'votanti === 'voti_si + 'voti_no + 'voti_bianchi + 'voti_nonvalidi + 'voti_contestati)
  }

  def extract(fileName: String): Dataset[DataPerComune] = {

    val df: DataFrame = spark.read
      .schema(schema)
      .options(csvOptions)
      .csv(resourcePath(fileName))

    val processedDF: DataFrame = process(df)

    processedDF.as[DataPerComune]

  }

}
