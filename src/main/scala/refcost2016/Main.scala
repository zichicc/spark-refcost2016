package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import utils._
import utils.Resources.resourcePath
import java.io.File
import org.apache.spark.sql.Dataset

object Main extends App with SparkSessionBuilder {

  import Extract._
  import Transform._
  import Save._

  // files and directories management
  val inputFileName: String = args.head
  val inputPath: String = resourcePath(inputFileName)
  val csvOptions = Map("header" -> "true", "delimiter" -> ";")
  val outputFolder: String = "output"
  def outDirFileList: Array[File] =
    new File(s".${File.separatorChar}$outputFolder").listFiles

  // data processing
  val inputData: Dataset[DataPerComune] = extract(inputPath)
  val outputData: Dataset[DataPerRegione] = manipulate(inputData)

  writeOutput(outputData, csvOptions, outputFolder)
  stopSparkSession()
  renameCsvFile(outDirFileList, outputFolder, inputFileName)
  deleteTempfiles(outDirFileList)
}
