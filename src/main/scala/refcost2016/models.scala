package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
final case class DataPerComune(regione: Option[String],
                               provincia: Option[String],
                               comune: Option[String],
                               elettori: Option[Long],
                               elettori_m: Option[Long],
                               votanti: Option[Long],
                               votanti_m: Option[Long],
                               votisi: Option[Long],
                               votino: Option[Long],
                               votibianchi: Option[Long],
                               votinonvalidi: Option[Long],
                               voticontestati: Option[Long])

final case class DataPerRegione(regione: String,
                                elettori_m: Long,
                                elettori_f: Long,
                                elettori: Long,
                                percvotanti: Double,
                                percvoti_si: Double,
                                percvoti_no: Double,
                                percvoti_bnvc: Double)
