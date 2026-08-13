"""
handler.py — Microservicio serverless de disparo de Spark (Etapa 2)
---------------------------------------------------------------------
AWS Lambda invocada por el JobDispatcherActor (Akka) una vez que la
Etapa 1 (GPU/OpenMP) terminó de preprocesar el array y lo dejó en S3.
Esta función lanza, vía EMR Serverless, DOS jobs de Spark sobre el
mismo dataset preprocesado: uno con la aplicación RDD (rdd_pipeline.py)
y otro con la aplicación DataFrame (dataframe_pipeline.py), para poder
comparar rendimiento (Etapa 2, punto 2 del enunciado).

Runtime: Python 3.12
Variables de entorno esperadas:
  EMR_APPLICATION_ID   ID de la aplicación EMR Serverless (Spark)
  EMR_EXECUTION_ROLE   ARN del rol de ejecución IAM
  SCRIPTS_BUCKET       Bucket S3 donde residen rdd_pipeline.py / dataframe_pipeline.py
  OUTPUT_BUCKET        Bucket S3 donde cada pipeline escribe su resultado
"""

import json
import os
import boto3

emr = boto3.client("emr-serverless")

EMR_APPLICATION_ID = os.environ.get("EMR_APPLICATION_ID", "")
EMR_EXECUTION_ROLE = os.environ.get("EMR_EXECUTION_ROLE", "")
SCRIPTS_BUCKET = os.environ.get("SCRIPTS_BUCKET", "finanzen-bigdata-scripts")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "finanzen-bigdata-results")


def _start_spark_job(job_name: str, script_key: str, input_path: str, output_path: str) -> str:
    response = emr.start_job_run(
        applicationId=EMR_APPLICATION_ID,
        executionRoleArn=EMR_EXECUTION_ROLE,
        name=job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": f"s3://{SCRIPTS_BUCKET}/{script_key}",
                "entryPointArguments": [input_path, output_path],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=2 "
                    "--conf spark.executor.memory=4g "
                    "--conf spark.driver.cores=1 "
                    "--conf spark.driver.memory=2g "
                    "--conf spark.dynamicAllocation.enabled=true"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{OUTPUT_BUCKET}/logs/"}
            }
        },
    )
    return response["jobRunId"]


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
        job_id = body.get("jobId", "unknown-job")
        preprocessed = body.get("preprocessedRef", {})
        # preprocessedRef viene del handler de la Etapa 1 (gpu-preprocess-lambda)
        s3_input = preprocessed.get("s3Location") or f"s3://{OUTPUT_BUCKET}/preprocessed/{job_id}.json"

        s3_output_rdd = f"s3://{OUTPUT_BUCKET}/results/{job_id}/rdd/"
        s3_output_df = f"s3://{OUTPUT_BUCKET}/results/{job_id}/dataframe/"

        rdd_run_id = _start_spark_job(f"{job_id}-rdd", "rdd_pipeline.py", s3_input, s3_output_rdd)
        df_run_id = _start_spark_job(f"{job_id}-dataframe", "dataframe_pipeline.py", s3_input, s3_output_df)

        return {
            "statusCode": 202,
            "body": json.dumps({
                "jobId": job_id,
                "s3InputPath": s3_input,
                "s3OutputPathRdd": s3_output_rdd,
                "s3OutputPathDf": s3_output_df,
                "emrRunIdRdd": rdd_run_id,
                "emrRunIdDataFrame": df_run_id,
                "status": "SPARK_JOBS_SUBMITTED",
            }),
        }
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": f"Error al lanzar jobs de Spark: {str(e)}"})}
