package orchestrator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import orchestrator.Protocol._

/**
 * InputValidatorActor
 * -----------------------------------------------------------------------
 * Primera etapa del pipeline de orquestación. Valida que el array recibido
 * por HTTP cumpla las restricciones de negocio antes de invocar cualquier
 * función serverless (evita gastar cómputo GPU/Spark en payloads inválidos).
 */
object InputValidatorActor {

  private val MaxElements = 50_000_000
  private val MinElements = 1

  def apply(): Behavior[ValidateInput] = Behaviors.receive { (context, msg) =>
    val ValidateInput(request, replyTo) = msg

    val result: ValidationResult =
      if (request.rawData.isEmpty)
        InputInvalid(request.jobId, "El array 'rawData' no puede estar vacío.")
      else if (request.rawData.length > MaxElements)
        InputInvalid(request.jobId, s"El array excede el máximo de $MaxElements elementos.")
      else if (request.rawData.exists(v => v.isNaN || v.isInfinite))
        InputInvalid(request.jobId, "El array contiene valores NaN o infinitos.")
      else
        InputValid(request)

    result match {
      case InputInvalid(_, reason) =>
        context.log.warn(s"[InputValidatorActor] Job ${request.jobId} rechazado: $reason")
      case InputValid(_) =>
        context.log.info(s"[InputValidatorActor] Job ${request.jobId} validado (${request.rawData.length} elementos)")
    }

    replyTo ! result
    Behaviors.same
  }
}
