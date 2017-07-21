package refcost2016.utils

/**
  * Created by christianzichichi <christianzichichi@gmail.com> on 19/07/2017.
  */
import java.nio.file.Paths

object Resources {

  def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(s"/$resource").toURI).toString

}
