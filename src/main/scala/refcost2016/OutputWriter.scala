package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils.SparkSessionBuilder
import org.apache.spark.sql.{Dataset, SaveMode}
import java.nio.file.Path

object OutputWriter extends App with SparkSessionBuilder {

  lazy val defaultFileName: String = "referendum2016.csv"
  val inputFileName: String = args.headOption.getOrElse(defaultFileName)
  val outputPath: String = s"output/$inputFileName-aggregated.csv"

  private val csvOptions =
    Map("header" -> "true", "delimiter" -> ";")

  val inputData: Dataset[DataPerComune] =
    Extraction.extract(inputFileName)
  val outputData: Dataset[DataPerRegione] = Manipulation.manipulate(inputData)

  outputData
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .options(csvOptions)
    .csv(outputPath)
}
