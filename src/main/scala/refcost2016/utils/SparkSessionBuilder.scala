package refcost2016.utils

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

trait SparkSessionBuilder {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName(this.getClass.getSimpleName)
    .getOrCreate()

}
