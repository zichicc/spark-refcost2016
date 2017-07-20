package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import org.apache.spark.sql.Dataset
import utils.SparkSessionBuilder

object OutputWriter extends App with SparkSessionBuilder {

  lazy val defaultFileName: String = "referendum2016.csv"
  val fileName: Option[String] = args.headOption
  private val csvOptions =
    Map("header" -> "true", "delimiter" -> ";")

  val inputData: Dataset[DataPerComune] =
    Extraction.extract(fileName.getOrElse(defaultFileName))
  val outputData: Dataset[DataPerRegione] = Manipulation.manipulate(inputData)

  outputData.printSchema

}
