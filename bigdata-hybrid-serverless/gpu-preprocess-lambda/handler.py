"""
handler.py — Microservicio serverless de preprocesamiento (Etapa 1)
---------------------------------------------------------------------
AWS Lambda que recibe un array numérico vía API Gateway (JSON body),
lo normaliza (Min-Max) invocando el binario nativo compilado a partir
de normalize.cu (GPU, cuando la función corre sobre una instancia con
soporte CUDA vía AWS Lambda + contenedor con GPU / EC2 g4dn detrás de
una cola SQS) o normalize_omp.c (fallback CPU/OpenMP), y publica el
resultado preprocesado en S3 para que la Etapa 2 (Spark) lo consuma.

Runtime: Python 3.12 (contenedor Lambda personalizado, imagen basada en
`public.ecr.aws/lambda/python:3.12` + capas CUDA cuando aplica).

Variables de entorno esperadas:
  OUTPUT_BUCKET       Bucket S3 donde se deposita el array preprocesado
  GPU_BINARY_PATH     Ruta al binario nativo (normalize_gpu o normalize_omp)
  USE_GPU             "true" | "false" — selección de binario en runtime
  ORCHESTRATOR_QUEUE  URL de la cola SQS que notifica al actor Akka
                       (JobDispatcherActor) que el preprocesamiento terminó
"""

import json
import os
import subprocess
import time
import uuid
import boto3

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "finanzen-bigdata-preprocessed")
GPU_BINARY_PATH = os.environ.get("GPU_BINARY_PATH", "/opt/bin/normalize_gpu")
CPU_BINARY_PATH = os.environ.get("CPU_BINARY_PATH", "/opt/bin/normalize_omp")
USE_GPU = os.environ.get("USE_GPU", "false").lower() == "true"
ORCHESTRATOR_QUEUE = os.environ.get("ORCHESTRATOR_QUEUE", "")


class ValidationError(Exception):
    pass


def _validate_payload(body: dict) -> list:
    if "data" not in body:
        raise ValidationError("El campo 'data' es requerido (array numérico).")
    data = body["data"]
    if not isinstance(data, list) or len(data) == 0:
        raise ValidationError("'data' debe ser un array numérico no vacío.")
    if len(data) > 50_000_000:
        raise ValidationError("El array excede el límite de 50M elementos por invocación.")
    for v in data[:100]:  # validación rápida de tipo sobre una muestra
        if not isinstance(v, (int, float)):
            raise ValidationError("Todos los elementos de 'data' deben ser numéricos.")
    return data


def _run_native_normalizer(data: list, job_id: str) -> dict:
    """
    Escribe el array a un archivo binario temporal, invoca el ejecutable
    nativo (GPU o CPU) compilado desde CUDA/OpenMP, y lee de vuelta el
    resultado normalizado junto con las métricas de tiempo.
    """
    import struct

    tmp_in = f"/tmp/{job_id}_in.bin"
    tmp_out = f"/tmp/{job_id}_out.bin"

    with open(tmp_in, "wb") as f:
        f.write(struct.pack(f"{len(data)}f", *[float(x) for x in data]))

    binary = GPU_BINARY_PATH if USE_GPU else CPU_BINARY_PATH
    t0 = time.perf_counter()

    # El binario nativo espera: <bin_in> <bin_out> <n_elementos>
    result = subprocess.run(
        [binary, tmp_in, tmp_out, str(len(data))],
        capture_output=True, text=True, timeout=840,  # margen bajo el límite de 15 min de Lambda
    )
    elapsed_ms = (time.perf_counter() - t0) * 1000

    if result.returncode != 0:
        raise RuntimeError(f"Fallo el binario nativo ({binary}): {result.stderr}")

    with open(tmp_out, "rb") as f:
        raw = f.read()
    normalized = list(struct.unpack(f"{len(data)}f", raw))

    os.remove(tmp_in)
    os.remove(tmp_out)

    return {
        "normalized": normalized,
        "engine": "gpu-cuda" if USE_GPU else "cpu-openmp",
        "processing_time_ms": round(elapsed_ms, 3),
    }


def lambda_handler(event, context):
    request_id = context.aws_request_id if context else str(uuid.uuid4())
    job_id = f"job-{request_id}"

    try:
        body = json.loads(event.get("body") or "{}")
        data = _validate_payload(body)

        outcome = _run_native_normalizer(data, job_id)

        s3_key = f"preprocessed/{job_id}.json"
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=s3_key,
            Body=json.dumps({
                "job_id": job_id,
                "n": len(data),
                "normalized": outcome["normalized"],
            }),
            ContentType="application/json",
        )

        # Notifica al orquestador Akka (vía SQS -> Akka Alpakka SQS source)
        # para que el JobDispatcherActor lance la Etapa 2 (Spark).
        if ORCHESTRATOR_QUEUE:
            sqs.send_message(
                QueueUrl=ORCHESTRATOR_QUEUE,
                MessageBody=json.dumps({
                    "type": "PreprocessingCompleted",
                    "jobId": job_id,
                    "s3Bucket": OUTPUT_BUCKET,
                    "s3Key": s3_key,
                    "n": len(data),
                    "engine": outcome["engine"],
                    "processingTimeMs": outcome["processing_time_ms"],
                }),
            )

        return {
            "statusCode": 202,
            "body": json.dumps({
                "jobId": job_id,
                "status": "PREPROCESSED",
                "engine": outcome["engine"],
                "processingTimeMs": outcome["processing_time_ms"],
                "s3Location": f"s3://{OUTPUT_BUCKET}/{s3_key}",
            }),
        }

    except ValidationError as e:
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
    except Exception as e:  # pragma: no cover — errores inesperados de infraestructura
        return {"statusCode": 500, "body": json.dumps({"error": f"Error interno: {str(e)}"})}
