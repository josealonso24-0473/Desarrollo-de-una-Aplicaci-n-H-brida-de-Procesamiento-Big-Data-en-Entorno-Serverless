"""
rdd_pipeline.py — Etapa 2 (Spark), variante RDD
---------------------------------------------------------------------
Job de Spark lanzado por la función serverless spark_trigger_lambda
(ver spark-jobs/lambda_spark_trigger/handler.py). Lee el dataset
preprocesado (salida de la Etapa 1, en HDFS o S3) y calcula, usando
únicamente la API de RDD (bajo nivel), el monto promedio normalizado
y el total transaccionado por categoría de comercio.

Uso:
  spark-submit rdd_pipeline.py <input_path> <output_path>

En AWS, <input_path> / <output_path> son rutas s3a://; en un clúster
on-prem, HDFS: hdfs:///data/preprocessed/...
"""
import sys
import time
import json
from pyspark import SparkContext, SparkConf


def run(input_path: str, output_path: str):
    conf = SparkConf().setAppName("FinanZen-BigData-RDD-Pipeline")
    sc = SparkContext.getOrCreate(conf)
    sc.setLogLevel("WARN")

    t0 = time.perf_counter()

    raw = sc.textFile(input_path)
    header = raw.first()
    rows = raw.filter(lambda line: line != header) \
               .map(lambda line: line.split(","))

    # (category, amount_normalized) -> pares clave/valor
    kv = rows.map(lambda cols: (cols[2], float(cols[3])))

    # Suma y conteo por clave, en un solo pase, con reduceByKey
    sum_count = kv.mapValues(lambda v: (v, 1)) \
                  .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    averages = sum_count.mapValues(lambda sc_: sc_[0] / sc_[1])
    totals = sum_count.mapValues(lambda sc_: sc_[0])

    avg_collected = dict(averages.collect())
    total_collected = dict(totals.collect())
    count_collected = dict(sum_count.mapValues(lambda sc_: sc_[1]).collect())

    elapsed = time.perf_counter() - t0

    result = {
        "pipeline": "RDD",
        "elapsed_seconds": round(elapsed, 4),
        "n_categories": len(avg_collected),
        "avg_normalized_by_category": avg_collected,
        "total_normalized_by_category": total_collected,
        "count_by_category": count_collected,
    }

    sc.parallelize([json.dumps(result)]).saveAsTextFile(output_path)
    print(json.dumps({"pipeline": "RDD", "elapsed_seconds": result["elapsed_seconds"]}))

    sc.stop()
    return result


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/preprocessed_transactions.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output/rdd_result"
    run(input_path, output_path)
