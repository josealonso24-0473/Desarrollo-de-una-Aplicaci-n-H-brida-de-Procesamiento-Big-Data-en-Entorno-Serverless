package orchestrator

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import orchestrator.Protocol._

/**
 * ResponseActor
 * -----------------------------------------------------------------------
 * Cuarta y última etapa: formatea el resultado consolidado en la
 * respuesta JSON final que se devuelve por el endpoint HTTP
 * GET /results/{jobId} (o como callback si el cliente usó un webhook).
 */
object ResponseActor {

  def apply(): Behavior[BuildResponse] = Behaviors.receive { (context, msg) =>
    val BuildResponse(jobId, analysis, replyTo) = msg

    val json =
      s"""{
         |  "jobId": "$jobId",
         |  "status": "COMPLETED",
         |  "performance": {
         |    "rddElapsedSeconds": ${analysis.rddElapsedSeconds},
         |    "dataFrameElapsedSeconds": ${analysis.dataFrameElapsedSeconds},
         |    "speedupDataFrameOverRdd": ${analysis.speedup},
         |    "categoriesAnalyzed": ${analysis.categoriesAnalyzed}
         |  }
         |}""".stripMargin

    context.log.info(s"[ResponseActor] Respuesta final construida para job $jobId")
    replyTo ! FinalResponse(jobId, statusCode = 200, jsonBody = json)
    Behaviors.same
  }
}
