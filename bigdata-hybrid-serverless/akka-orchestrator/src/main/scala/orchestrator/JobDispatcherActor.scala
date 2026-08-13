package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import orchestrator.Protocol._

/**
 * JobDispatcherActor
 * -----------------------------------------------------------------------
 * Segunda etapa: invoca, en secuencia, la función Lambda de preprocesamiento
 * GPU (Etapa 1) y luego la función Lambda que lanza el job de Spark
 * (Etapa 2, RDD + DataFrame) sobre el resultado preprocesado en S3.
 *
 * Política de reintentos: backoff exponencial con un máximo de 3 intentos
 * por invocación HTTP a las funciones Lambda (API Gateway), consistente
 * con el requisito "controla errores y aplica retries (modelo actor)".
 */
object JobDispatcherActor {

  private val MaxRetries = 3
  private val InitialBackoff = 500.millis

  private val gpuLambdaEndpoint = System.getenv().getOrDefault(
    "GPU_LAMBDA_ENDPOINT", "https://api.execute-api.us-east-1.amazonaws.com/prod/preprocess")
  private val sparkLambdaEndpoint = System.getenv().getOrDefault(
    "SPARK_LAMBDA_ENDPOINT", "https://api.execute-api.us-east-1.amazonaws.com/prod/spark-job")

  def apply(): Behavior[DispatchJob] = Behaviors.setup { context =>
    implicit val system = context.system
    dispatching(context)
  }

  private def dispatching(context: ActorContext[DispatchJob]): Behavior[DispatchJob] =
    Behaviors.receiveMessage { case DispatchJob(request, replyTo) =>
      attemptDispatch(context, request, replyTo, attempt = 1)
      Behaviors.same
    }

  private def attemptDispatch(
      context: ActorContext[DispatchJob],
      request: Protocol.ProcessRequest,
      replyTo: ActorRef[DispatchResult],
      attempt: Int
  ): Unit = {
    import context.executionContext
    implicit val system = context.system

    val gpuPayload = HttpEntity(
      ContentTypes.`application/json`,
      s"""{"data": [${request.rawData.mkString(",")}]}"""
    )

    val gpuRequest = HttpRequest(method = HttpMethods.POST, uri = gpuLambdaEndpoint, entity = gpuPayload)

    val resultFuture = for {
      gpuResponse <- Http(system).singleRequest(gpuRequest)
      gpuBody <- Unmarshal(gpuResponse.entity).to[String]
      sparkPayload = HttpEntity(
        ContentTypes.`application/json`,
        s"""{"jobId": "${request.jobId}", "preprocessedRef": $gpuBody}"""
      )
      sparkRequest = HttpRequest(method = HttpMethods.POST, uri = sparkLambdaEndpoint, entity = sparkPayload)
      sparkResponse <- Http(system).singleRequest(sparkRequest)
      sparkBody <- Unmarshal(sparkResponse.entity).to[String]
    } yield (gpuBody, sparkBody)

    resultFuture.onComplete {
      case Success((_, sparkBody)) =>
        // sparkBody se asume con forma:
        // {"jobId":..,"s3InputPath":..,"s3OutputPathRdd":..,"s3OutputPathDf":..}
        val parsed = JobResponseParser.parse(sparkBody)
        parsed match {
          case Some(p) =>
            replyTo ! JobDispatched(request.jobId, p.s3InputPath, p.s3OutputPathRdd, p.s3OutputPathDf)
          case None =>
            handleFailure(context, request, replyTo, attempt, "Respuesta de Spark Lambda con formato inesperado")
        }
      case Failure(ex) =>
        handleFailure(context, request, replyTo, attempt, ex.getMessage)
    }
  }

  private def handleFailure(
      context: ActorContext[DispatchJob],
      request: Protocol.ProcessRequest,
      replyTo: ActorRef[DispatchResult],
      attempt: Int,
      reason: String
  ): Unit = {
    if (attempt >= MaxRetries) {
      context.log.error(s"[JobDispatcherActor] Job ${request.jobId} agotó reintentos ($attempt): $reason")
      replyTo ! JobDispatchFailed(request.jobId, reason, attempt)
    } else {
      val backoff = InitialBackoff * math.pow(2, attempt - 1).toLong
      context.log.warn(s"[JobDispatcherActor] Job ${request.jobId} falló intento $attempt: $reason. " +
        s"Reintentando en ${backoff.toMillis}ms")
      context.scheduleOnce(backoff, context.self, DispatchJob(request, replyTo)) // reintento vía mensaje programado
      ()
    }
  }
}

/** Utilidad mínima de parsing (evita traer una librería JSON completa solo para este ejemplo). */
object JobResponseParser {
  case class Parsed(s3InputPath: String, s3OutputPathRdd: String, s3OutputPathDf: String)

  def parse(json: String): Option[Parsed] = {
    // En producción: usar spray-json / circe. Aquí, extracción simple para
    // mantener el ejemplo autocontenido y legible.
    def extract(key: String): Option[String] =
      "\"" + key + "\"\\s*:\\s*\"([^\"]*)\"".r.findFirstMatchIn(json).map(_.group(1))
    for {
      in <- extract("s3InputPath")
      rdd <- extract("s3OutputPathRdd")
      df <- extract("s3OutputPathDf")
    } yield Parsed(in, rdd, df)
  }
}
