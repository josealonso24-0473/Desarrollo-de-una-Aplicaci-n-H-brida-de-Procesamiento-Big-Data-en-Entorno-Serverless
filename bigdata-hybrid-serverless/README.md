# Aplicación Híbrida de Procesamiento Big Data en Entorno Serverless

Proyecto integrador que combina **GPU (CUDA/OpenMP)**, **Apache Spark** (RDD y
DataFrame), **modelo de actores (Akka)** y **arquitectura serverless (AWS
Lambda + EMR Serverless + Fargate + API Gateway)**.

## Arquitectura (resumen)

```
Cliente HTTP
    │  POST /process { "data": [...] }
    ▼
API Gateway ── VPC Link ──▶ Orquestador Akka (Fargate, puerto 8080)
                                 │
                 ┌───────────────┼────────────────────────────┐
                 ▼               ▼                             ▼
      InputValidatorActor  JobDispatcherActor           ResultAnalyzerActor ─▶ ResponseActor
                                 │  (retry backoff x3)          │ (polling x5, 3s)
                                 ▼                               │
                    ┌────────────────────────┐                  │
                    ▼                        ▼                  │
        Lambda: gpu-preprocess     Lambda: spark-trigger         │
        (CUDA/OpenMP, normaliza)   (EMR Serverless: lanza         │
              │                     rdd_pipeline.py Y              │
              ▼                     dataframe_pipeline.py)          │
         S3 (preprocessed/)              │                          │
                                          ▼                          │
                                   S3 (results/{jobId}/rdd|df) ──────┘
```

Cada etapa del enunciado corresponde a un directorio:

| Etapa | Directorio | Descripción |
|---|---|---|
| 1. Preprocesamiento GPU | `gpu-preprocess-lambda/` | Kernel CUDA (`cuda/normalize.cu`) + fallback OpenMP (`cuda/normalize_omp_io.c`) + handler Lambda (`handler.py`) |
| 2. Procesamiento Spark | `spark-jobs/` | `rdd_pipeline.py`, `dataframe_pipeline.py`, `lambda_spark_trigger/handler.py` |
| 3. Orquestación con actores | `akka-orchestrator/` | Sistema de actores Akka Typed (Scala) |
| 4. Flujo HTTP + infraestructura | `infra/template.yaml` | AWS SAM: Lambdas, buckets S3, cola SQS, roles IAM |
| Datos de prueba | `data/generate_dataset.py` | Generador de dataset sintético (transacciones de MiPymes) |
| Resultados de benchmark reales | `benchmarks/` | CSVs con mediciones reales ejecutadas en este entorno de desarrollo |
| Informe y video | `docs/` | Informe de arquitectura/rendimiento y guion de video |

## ⚠️ Nota importante sobre el entorno de ejecución

Este repositorio fue desarrollado y probado en un entorno de desarrollo (sandbox)
**sin GPU física, sin CUDA Toolkit (`nvcc`), sin clúster EMR/AWS real y sin
`sbt`/Scala instalado**. Por transparencia académica:

- **Sí se compiló y ejecutó realmente**: la variante OpenMP de la normalización
  (`gcc -fopenmp`), con resultados en `benchmarks/openmp_results.csv`.
- **Sí se ejecutó realmente**: ambos pipelines de Spark (`rdd_pipeline.py` y
  `dataframe_pipeline.py`) en modo local (`local[1]`, PySpark 4.2.0, un solo
  núcleo disponible), con resultados reales en `benchmarks/spark_results.csv`.
- **No se pudo compilar/ejecutar en este entorno** (por ausencia de hardware/
  herramientas, no por limitación de diseño): el kernel CUDA (`normalize.cu`,
  requiere GPU NVIDIA + `nvcc`), el sistema de actores Akka (requiere `sbt` +
  descarga de artefactos de Maven Central, no accesible desde este sandbox),
  y el despliegue real en AWS (Lambda, EMR Serverless, Fargate, API Gateway
  — requiere credenciales de una cuenta AWS). Este código sigue los patrones
  estándar de cada tecnología y está listo para desplegarse siguiendo los
  pasos de la sección "Despliegue" a continuación; el informe (`docs/informe.docx`)
  documenta esta limitación honestamente en la sección de metodología.

Este mismo patrón se siguió en los proyectos previos del curso de
Programación Distribuida (MPI/OpenMP), donde los sandboxes de desarrollo
también estuvieron limitados en núcleos de CPU disponibles.

## Cómo ejecutar cada componente

### 1. Preprocesamiento (OpenMP, localmente)
```bash
cd gpu-preprocess-lambda/cuda
gcc -O3 -fopenmp -o normalize_omp_io normalize_omp_io.c -lm
./normalize_omp_io entrada.bin salida.bin <n_elementos>
```
Para la variante GPU real (en una máquina con CUDA Toolkit y GPU NVIDIA):
```bash
nvcc -O3 -arch=sm_75 -o normalize_gpu normalize.cu
```

### 2. Pipelines de Spark (localmente, con PySpark instalado)
```bash
pip install pyspark
python3 data/generate_dataset.py 500000 data/preprocessed_transactions.csv
spark-submit spark-jobs/rdd_pipeline.py data/preprocessed_transactions.csv output/rdd_result
spark-submit spark-jobs/dataframe_pipeline.py data/preprocessed_transactions.csv output/dataframe_result
```

### 3. Orquestador Akka (requiere sbt + JDK 21)
```bash
cd akka-orchestrator
sbt run
# Servidor disponible en http://localhost:8080
curl -X POST http://localhost:8080/process -d '{"data":[1,2,3,4,5]}'
curl http://localhost:8080/results/<jobId>
```

### 4. Despliegue serverless completo en AWS
```bash
cd infra
sam build
sam deploy --guided \
  --parameter-overrides EmrServerlessApplicationId=<id-de-tu-app-emr>
# Luego construir y publicar la imagen del orquestador Akka a ECR,
# y crear el servicio Fargate + VPC Link hacia API Gateway.
docker build -t bigdata-orchestrator ./akka-orchestrator
```

## Deliverables incluidos
- [x] Código: CUDA/OpenMP, scripts Spark, sistema de actores Akka, funciones serverless
- [x] Informe (`docs/informe.docx`): arquitectura, código, análisis de rendimiento, conclusiones
- [x] Guion de video demostrativo (`docs/guion_video.md`)
- [x] Repositorio con instrucciones detalladas (este archivo)
