package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
object Models {

  final case class DataPerComune(descregione: String,
                                 descprovincia: String,
                                 desccomune: String,
                                 elettori: Long,
                                 elettori_m: Long,
                                 votanti: Long,
                                 votanti_m: Long,
                                 numvotisi: Long,
                                 numvotino: Long,
                                 numvotibianchi: Long,
                                 numvotinonvalidi: Long,
                                 numvoticontestati: Long)

  final case class DataPerRegione(descregione: String,
                                  elettori_m: Long,
                                  elettori_f: Long,
                                  elettori: Long,
                                  percvotanti: Double,
                                  percvotanti_si: Double,
                                  percvotanti_no: Double,
                                  percvotanti_bnc: Double)

}
