package orchestrator

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import orchestrator.Protocol._

/**
 * ResultAnalyzerActor
 * -----------------------------------------------------------------------
 * Tercera etapa: espera (con polling y reintentos) a que ambos resultados
 * de Spark (RDD y DataFrame) estén disponibles en S3, los descarga,
 * calcula el speedup relativo, y produce el resumen de análisis.
 *
 * Usa Behaviors.withTimers para reintentar el polling sin bloquear el
 * hilo del actor (los jobs de Spark corren de forma asíncrona en EMR).
 */
object ResultAnalyzerActor {

  private val MaxPollAttempts = 5
  private val PollInterval = 3.seconds
  private case object PollTick

  def apply(): Behavior[AnalyzeResults] = Behaviors.receive { (context, msg) =>
    val AnalyzeResults(jobId, rddPath, dfPath, replyTo) = msg
    pollForResults(context, jobId, rddPath, dfPath, replyTo, attempt = 1)
    Behaviors.same
  }

  private def pollForResults(
      context: ActorContext[AnalyzeResults],
      jobId: String,
      rddPath: String,
      dfPath: String,
      replyTo: ActorRef[AnalysisResult],
      attempt: Int
  ): Unit = {
    // fetchS3Json simula/realiza la lectura del objeto JSON de resultados
    // que cada pipeline de Spark escribió en S3 (ver rdd_pipeline.py /
    // dataframe_pipeline.py, campo "elapsed_seconds").
    val rddResult = S3ResultReader.tryRead(rddPath)
    val dfResult = S3ResultReader.tryRead(dfPath)

    (rddResult, dfResult) match {
      case (Success(rdd), Success(df)) =>
        val speedup = if (df.elapsedSeconds > 0) df.elapsedSeconds / rdd.elapsedSeconds else 0.0
        context.log.info(s"[ResultAnalyzerActor] Job $jobId analizado: " +
          s"RDD=${rdd.elapsedSeconds}s DataFrame=${df.elapsedSeconds}s speedup=$speedup")
        replyTo ! AnalysisSucceeded(
          jobId = jobId,
          rddElapsedSeconds = rdd.elapsedSeconds,
          dataFrameElapsedSeconds = df.elapsedSeconds,
          speedup = speedup,
          categoriesAnalyzed = math.max(rdd.categories, df.categories)
        )

      case _ if attempt >= MaxPollAttempts =>
        val reason = "Resultados de Spark no disponibles en S3 tras agotar reintentos de polling"
        context.log.error(s"[ResultAnalyzerActor] Job $jobId: $reason")
        replyTo ! AnalysisFailed(jobId, reason, attempt)

      case _ =>
        context.log.info(s"[ResultAnalyzerActor] Job $jobId: resultados aún no disponibles, " +
          s"reintento ${attempt + 1}/$MaxPollAttempts en ${PollInterval.toSeconds}s")
        context.scheduleOnce(
          PollInterval,
          context.self,
          AnalyzeResults(jobId, rddPath, dfPath, replyTo)
        )
    }
  }
}

/** Lector de resultados de S3 (abstrae AWS SDK para mantener el actor testeable). */
object S3ResultReader {
  case class SparkJobResult(elapsedSeconds: Double, categories: Int)

  def tryRead(s3Path: String): Try[SparkJobResult] = Try {
    // Implementación real: AmazonS3ClientBuilder.defaultClient().getObject(...)
    // seguido de parseo del JSON producido por rdd_pipeline.py / dataframe_pipeline.py.
    // Se deja como stub documentado; ver benchmarks/spark_results.csv para los
    // valores reales medidos en el entorno de desarrollo (modo local).
    throw new NotImplementedError(
      "Reemplazar por lectura real vía AWS SDK (S3Client.getObject) en despliegue.")
  }
}
