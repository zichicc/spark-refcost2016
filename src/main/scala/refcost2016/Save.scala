package refcost2016

import java.io.File

import org.apache.spark.sql.{Dataset, SaveMode}
import refcost2016.utils.{DataPerRegione, SparkSessionBuilder}

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 21/07/2017.
  */
object Save extends SparkSessionBuilder {

  def writeOutput(ds: Dataset[DataPerRegione],
                  csvOptions: Map[String, String],
                  outputFolder: String): Unit =
    ds.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(csvOptions)
      .csv(outputFolder)

  def stopSparkSession(): Unit = spark.stop

  def renameCsvFile(fl: Array[File],
                    outputFolder: String,
                    inputFileName: String): Unit =
    fl.find(_.getName.endsWith(".csv"))
      .foreach(_.renameTo(
        new File(s".${File.separatorChar}$outputFolder${File.separatorChar}${inputFileName
          .stripSuffix(".csv")}-aggregated.csv")))

  def deleteTempfiles(fl: Array[File]): Unit =
    fl.filter(!_.getName.endsWith(".csv")).foreach(_.delete)

}
