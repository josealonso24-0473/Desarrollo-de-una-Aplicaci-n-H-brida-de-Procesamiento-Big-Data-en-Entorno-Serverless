package orchestrator

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.util.Timeout
import spray.json._
import scala.concurrent.duration._
import scala.util.{Success, Failure}
import java.util.UUID
import orchestrator.Protocol._

/**
 * Main.scala — Etapa 4: endpoints HTTP + arranque del sistema de actores
 * -----------------------------------------------------------------------
 * Expone:
 *   POST /process          -> inicia el flujo completo (validación, GPU,
 *                              Spark RDD+DataFrame, análisis, respuesta)
 *   GET  /results/{jobId}   -> consulta el estado/resultado de un job
 *
 * Despliegue: este servicio Akka HTTP corre como contenedor de larga
 * duración en AWS Fargate (ECS), detrás de un Application Load Balancer
 * integrado a API Gateway (HTTP API + VPC Link), lo que permite mantener
 * el ActorSystem persistente (los actores no son aptos para el modelo
 * de ejecución efímera de Lambda) mientras las etapas de cómputo pesado
 * (GPU y Spark) sí se ejecutan como funciones serverless independientes,
 * cumpliendo el requisito de "aprovechar la naturaleza serverless para
 * escalar bajo demanda" en las etapas que lo permiten.
 */
object Main extends App {

  implicit val system: ActorSystem[Any] = ActorSystem(OrchestratorActor(), "bigdata-orchestrator")
  implicit val ec = system.executionContext
  implicit val timeout: Timeout = Timeout(30.seconds)

  val route =
    path("process") {
      post {
        entity(as[String]) { body =>
          val jobId = s"job-${UUID.randomUUID()}"
          val rawData = ProcessRequestJson.parseDataArray(body)

          val futureResponse = system.ask[FinalResponse] { replyTo =>
            OrchestratorActor.StartProcessing(ProcessRequest(jobId, rawData), replyTo)
          }

          onComplete(futureResponse) {
            case Success(resp) =>
              complete(HttpResponse(
                status = resp.statusCode,
                entity = HttpEntity(ContentTypes.`application/json`, resp.jsonBody)
              ))
            case Failure(ex) =>
              complete(HttpResponse(
                status = StatusCodes.GatewayTimeout,
                entity = HttpEntity(ContentTypes.`application/json`,
                  s"""{"jobId":"$jobId","status":"TIMEOUT","reason":"${ex.getMessage}"}""")
              ))
          }
        }
      }
    } ~
    path("results" / Segment) { jobId =>
      get {
        val futureState = system.ask[Option[OrchestratorActor.JobState]] { replyTo =>
          OrchestratorActor.GetJobState(jobId, replyTo)
        }
        onComplete(futureState) {
          case Success(Some(OrchestratorActor.Completed(resp))) =>
            complete(HttpEntity(ContentTypes.`application/json`, resp.jsonBody))
          case Success(Some(OrchestratorActor.InProgress)) =>
            complete(StatusCodes.Accepted -> s"""{"jobId":"$jobId","status":"IN_PROGRESS"}""")
          case Success(Some(OrchestratorActor.Failed(reason))) =>
            complete(StatusCodes.InternalServerError -> s"""{"jobId":"$jobId","status":"FAILED","reason":"$reason"}""")
          case Success(None) =>
            complete(StatusCodes.NotFound -> s"""{"jobId":"$jobId","status":"NOT_FOUND"}""")
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError -> s"""{"error":"${ex.getMessage}"}""")
        }
      }
    } ~
    path("health") {
      get { complete(StatusCodes.OK -> """{"status":"UP"}""") }
    }

  Http().newServerAt("0.0.0.0", 8080).bind(route)
  println("Orquestador Akka HTTP escuchando en :8080 (/process, /results/{jobId}, /health)")
}

/** Parser mínimo del body de entrada { "data": [ ... ] } */
object ProcessRequestJson {
  def parseDataArray(body: String): Seq[Double] = {
    val inner = body.trim.stripPrefix("{").stripSuffix("}")
    val arrPart = inner.split("\"data\"\\s*:\\s*\\[", 2)(1).split("\\]", 2)(0)
    if (arrPart.trim.isEmpty) Seq.empty
    else arrPart.split(",").map(_.trim.toDouble).toSeq
  }
}
