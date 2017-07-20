package refcost2016

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
final case class DataPerComune(regione: String,
                               provincia: String,
                               comune: String,
                               elettori: Long,
                               elettori_m: Long,
                               votanti: Long,
                               votanti_m: Long,
                               voti_si: Long,
                               voti_no: Long,
                               voti_bianchi: Long,
                               voti_nonvalidi: Long,
                               voti_contestati: Long)

final case class DataPerRegione(regione: String,
                                elettori_m: Long,
                                elettori_f: Long,
                                elettori: Long,
                                percvotanti: Double,
                                percvoti_si: Double,
                                percvoti_no: Double,
                                percvoti_bnvc: Double)
