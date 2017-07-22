package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 21/07/2017.
  */
import utils.{DataPerRegione, SparkSessionBuilder}
import java.io.File
import org.apache.spark.sql.{Dataset, SaveMode}

object Save extends SparkSessionBuilder {

  def stopSparkSession(): Unit = spark.stop

  def listFilesInDir(outputFolder: String): Array[File] =
    new File(s".${File.separatorChar}$outputFolder").listFiles

  def writeOutput(ds: Dataset[DataPerRegione],
                  csvOptions: Map[String, String],
                  outputFolder: String): Unit =
    ds.repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(csvOptions)
      .csv(outputFolder)

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
