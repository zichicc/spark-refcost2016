package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 20/07/2017.
  */
import utils._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DoubleType

object Transform extends SparkSessionBuilder {

  import spark.implicits._

  def transform(ds: Dataset[DataPerComune]): Dataset[DataPerRegione] =
    ds.drop("provincia", "comune")
      .groupBy('regione)
      .sum()
      .toDF("regione",
            "elettori",
            "elettori_m",
            "votanti",
            "votanti_m",
            "voti_si",
            "voti_no",
            "voti_bianchi",
            "voti_nonvalidi",
            "voti_contestati")
      .select(
        'regione,
        'elettori_m,
        'elettori - 'elettori_m as 'elettori_f,
        'elettori,
        ('votanti * 100).cast(DoubleType) / 'elettori as 'percvotanti,
        ('voti_si * 100).cast(DoubleType) / 'votanti as 'percvoti_si,
        ('voti_no * 100).cast(DoubleType) / 'votanti as 'percvoti_no,
        (('voti_bianchi + 'voti_nonvalidi + 'voti_contestati) * 100)
          .cast(DoubleType) / 'votanti as 'percvoti_bnvc
      )
      .sort('regione)
      .as[DataPerRegione]
}
