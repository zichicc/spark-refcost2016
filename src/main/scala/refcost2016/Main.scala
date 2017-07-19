package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils.SparkSessionBuilder

object Main extends App with SparkSessionBuilder {

  import spark.implicits._

  lazy val defaultFileName: String = "referendum2016.csv"
  lazy val fileName: Option[String] = args.headOption
  lazy val inputData =
    Extraction.extract(fileName.getOrElse(defaultFileName))

  //inputData.printSchema()

  inputData
    .select($"descregione", $"descprovincia", $"desccomune")
    .where(
      $"descregione" === "" or $"descprovincia" === "" or $"desccomune" === "")
    .show

}
