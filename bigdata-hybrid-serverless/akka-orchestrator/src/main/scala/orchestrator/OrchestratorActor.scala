package orchestrator

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import scala.concurrent.duration._
import scala.collection.mutable
import orchestrator.Protocol._

/**
 * OrchestratorActor
 * -----------------------------------------------------------------------
 * Actor raíz que crea y coordina a los cuatro actores de etapa
 * (InputValidatorActor, JobDispatcherActor, ResultAnalyzerActor,
 * ResponseActor), aplicando supervisión con estrategia de reinicio
 * ante fallos no controlados de cualquiera de ellos (además de los
 * reintentos explícitos de red que ya implementa JobDispatcherActor).
 *
 * Mantiene en memoria el estado de cada job (jobId -> estado actual)
 * para responder a GET /results/{jobId} mientras el procesamiento
 * está en curso ("persistencia" del flujo — Etapa 4 del enunciado).
 */
object OrchestratorActor {

  sealed trait JobState
  case object InProgress extends JobState
  final case class Completed(response: FinalResponse) extends JobState
  final case class Failed(reason: String) extends JobState

  // Comando externo para iniciar un procesamiento end-to-end
  final case class StartProcessing(request: ProcessRequest, replyTo: ActorRef[FinalResponse])
  final case class GetJobState(jobId: String, replyTo: ActorRef[Option[JobState]])

  private val jobStates = mutable.Map.empty[String, JobState]

  def apply(): Behavior[Any] = Behaviors.setup { context =>
    val validator = context.spawn(
      Behaviors.supervise(InputValidatorActor()).onFailure[Exception](SupervisorStrategy.restart),
      "input-validator")
    val dispatcher = context.spawn(
      Behaviors.supervise(JobDispatcherActor()).onFailure[Exception](
        SupervisorStrategy.restartWithBackoff(200.millis, 5.seconds, 0.2)),
      "job-dispatcher")
    val analyzer = context.spawn(
      Behaviors.supervise(ResultAnalyzerActor()).onFailure[Exception](SupervisorStrategy.restart),
      "result-analyzer")
    val responder = context.spawn(ResponseActor(), "response-builder")

    running(context, validator, dispatcher, analyzer, responder)
  }

  private def running(
      context: ActorContext[Any],
      validator: ActorRef[ValidateInput],
      dispatcher: ActorRef[DispatchJob],
      analyzer: ActorRef[AnalyzeResults],
      responder: ActorRef[BuildResponse]
  ): Behavior[Any] = Behaviors.receiveMessage {

    case StartProcessing(request, replyTo) =>
      jobStates(request.jobId) = InProgress

      // Adaptador: recibe ValidationResult y continúa el flujo
      val validationAdapter = context.messageAdapter[ValidationResult] {
        case InputValid(req) => InternalDispatch(req, replyTo)
        case InputInvalid(jobId, reason) => InternalFail(jobId, s"Validación fallida: $reason", replyTo)
      }
      validator ! ValidateInput(request, validationAdapter)
      Behaviors.same

    case InternalDispatch(request, replyTo) =>
      val dispatchAdapter = context.messageAdapter[DispatchResult] {
        case JobDispatched(jobId, in, rdd, df) => InternalAnalyze(jobId, rdd, df, replyTo)
        case JobDispatchFailed(jobId, reason, attempt) =>
          InternalFail(jobId, s"Fallo al emitir job tras $attempt intentos: $reason", replyTo)
      }
      dispatcher ! DispatchJob(request, dispatchAdapter)
      Behaviors.same

    case InternalAnalyze(jobId, rdd, df, replyTo) =>
      val analysisAdapter = context.messageAdapter[AnalysisResult] {
        case a: AnalysisSucceeded => InternalRespond(a, replyTo)
        case AnalysisFailed(id, reason, attempt) =>
          InternalFail(id, s"Análisis fallido tras $attempt intentos: $reason", replyTo)
      }
      analyzer ! AnalyzeResults(jobId, rdd, df, analysisAdapter)
      Behaviors.same

    case InternalRespond(analysis, replyTo) =>
      val responseAdapter = context.messageAdapter[FinalResponse] { resp =>
        jobStates(analysis.jobId) = Completed(resp)
        InternalDeliver(resp, replyTo)
      }
      responder ! BuildResponse(analysis.jobId, analysis, responseAdapter)
      Behaviors.same

    case InternalDeliver(response, replyTo) =>
      replyTo ! response
      Behaviors.same

    case InternalFail(jobId, reason, replyTo) =>
      jobStates(jobId) = Failed(reason)
      context.log.error(s"[OrchestratorActor] Job $jobId falló definitivamente: $reason")
      replyTo ! FinalResponse(jobId, 500, s"""{"jobId":"$jobId","status":"FAILED","reason":"$reason"}""")
      Behaviors.same

    case GetJobState(jobId, replyTo) =>
      replyTo ! jobStates.get(jobId)
      Behaviors.same

    case _ => Behaviors.same
  }

  // Mensajes internos de continuación (patrón "pipe/adapter" de Akka Typed)
  private final case class InternalDispatch(request: ProcessRequest, replyTo: ActorRef[FinalResponse])
  private final case class InternalAnalyze(jobId: String, rdd: String, df: String, replyTo: ActorRef[FinalResponse])
  private final case class InternalRespond(analysis: AnalysisSucceeded, replyTo: ActorRef[FinalResponse])
  private final case class InternalDeliver(response: FinalResponse, replyTo: ActorRef[FinalResponse])
  private final case class InternalFail(jobId: String, reason: String, replyTo: ActorRef[FinalResponse])
}
