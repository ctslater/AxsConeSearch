
package axs


// import axs.AxsConeSearch
import org.rogach.scallop._

// import Ordering.Implicits._

class AxsConeConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val server = opt[Boolean]()
  val port = opt[Int](default=Some(8080))
  val filename = trailArg[String]()
  verify()
}


object AxsConeMain {

  def main(args: Array[String]) {
    val conf = new AxsConeConf(args)

    if(conf.server()) {
      VOServer.start(conf.filename(), conf.port())

    } else {

      val cone_radius = 3/3600.0
      val center_ra = 287.9869104
      val center_dec = 13.0748496
      val search = new AxsConeSearch(conf.filename())
      search.search(center_ra, center_dec, cone_radius)

    }
  }
}

