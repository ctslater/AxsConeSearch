
package axs

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.StreamConverters.fromOutputStream
import scala.io.StdIn

case class ConeQueryParams(ra : Double, dec : Double, verb : Int)


class VOServer(val filename : String, implicit val system:ActorSystem,
  implicit val materializer:ActorMaterializer) {

  val executionContext = system.dispatcher

  val conesearch = new AxsConeSearch(filename)

  val route =
    path("cone") {
      parameters('RA.as[Double], 'DEC.as[Double], 'VERB.as[Int]).as(ConeQueryParams) {
        query => complete {
            conesearch.search(query.ra, query.dec, 1.5/3600.0).toByteArray
        }
      }
    }
}

object VOServer {

  def start(filename : String) =  {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val server = new VOServer(filename, system, materializer)
    val bindingFuture = Http().bindAndHandle(server.route, "localhost", 8080)


    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }
}
