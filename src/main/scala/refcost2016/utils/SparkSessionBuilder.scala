package refcost2016.utils

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionBuilder {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  private lazy val conf: SparkConf =
    new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-XX:+UseCompressedOops")

  implicit val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .master("local[*]")
    .appName("spark-refcost2016")
    .getOrCreate()
}
