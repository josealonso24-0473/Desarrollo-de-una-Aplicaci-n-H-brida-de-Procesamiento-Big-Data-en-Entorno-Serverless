package orchestrator

import java.time.Instant

/**
 * Protocol.scala
 * -----------------------------------------------------------------------
 * Mensajes intercambiados entre los actores del sistema de orquestación
 * (Etapa 3 del proyecto). Cada actor gestiona una etapa del flujo:
 *
 *   HTTP request -> InputValidatorActor -> JobDispatcherActor
 *                -> (Lambda GPU + Lambda Spark, vía SQS/HTTP)
 *                -> ResultAnalyzerActor -> ResponseActor -> HTTP response
 */
object Protocol {

  // -------------------- Comandos de entrada --------------------

  /** Petición inicial recibida por el endpoint HTTP POST /process */
  final case class ProcessRequest(
    jobId: String,
    rawData: Seq[Double],
    submittedAt: Instant = Instant.now()
  )

  // -------------------- Etapa: validación --------------------

  final case class ValidateInput(request: ProcessRequest, replyTo: akka.actor.typed.ActorRef[ValidationResult])
  sealed trait ValidationResult
  final case class InputValid(request: ProcessRequest) extends ValidationResult
  final case class InputInvalid(jobId: String, reason: String) extends ValidationResult

  // -------------------- Etapa: emisión del job (GPU + Spark) --------------------

  final case class DispatchJob(request: ProcessRequest, replyTo: akka.actor.typed.ActorRef[DispatchResult])
  sealed trait DispatchResult
  final case class JobDispatched(jobId: String, s3InputPath: String, s3OutputPathRdd: String, s3OutputPathDf: String) extends DispatchResult
  final case class JobDispatchFailed(jobId: String, reason: String, attempt: Int) extends DispatchResult

  // -------------------- Etapa: análisis de resultados --------------------

  final case class AnalyzeResults(jobId: String, s3OutputPathRdd: String, s3OutputPathDf: String, replyTo: akka.actor.typed.ActorRef[AnalysisResult])
  sealed trait AnalysisResult
  final case class AnalysisSucceeded(
    jobId: String,
    rddElapsedSeconds: Double,
    dataFrameElapsedSeconds: Double,
    speedup: Double,
    categoriesAnalyzed: Int
  ) extends AnalysisResult
  final case class AnalysisFailed(jobId: String, reason: String, attempt: Int) extends AnalysisResult

  // -------------------- Etapa: respuesta final --------------------

  final case class BuildResponse(jobId: String, analysis: AnalysisSucceeded, replyTo: akka.actor.typed.ActorRef[FinalResponse])
  final case class FinalResponse(jobId: String, statusCode: Int, jsonBody: String)

  // -------------------- Errores y reintentos --------------------

  final case class RetryExceeded(jobId: String, stage: String, lastReason: String)
}
