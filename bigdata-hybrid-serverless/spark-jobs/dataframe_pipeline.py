"""
dataframe_pipeline.py — Etapa 2 (Spark), variante DataFrame
---------------------------------------------------------------------
Equivalente funcional de rdd_pipeline.py, usando la API estructurada
de DataFrame/Spark SQL, que se beneficia del optimizador Catalyst y
de Tungsten (ejecución en formato binario columnar) para comparar
rendimiento contra la variante RDD sobre el mismo dataset.

Uso:
  spark-submit dataframe_pipeline.py <input_path> <output_path>
"""
import sys
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("FinanZen-BigData-DataFrame-Pipeline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.perf_counter()

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

    agg = df.groupBy("category").agg(
        F.avg("amount_normalized").alias("avg_normalized"),
        F.sum("amount_normalized").alias("total_normalized"),
        F.count("*").alias("count"),
    )

    collected = agg.collect()
    elapsed = time.perf_counter() - t0

    avg_by_cat = {r["category"]: r["avg_normalized"] for r in collected}
    total_by_cat = {r["category"]: r["total_normalized"] for r in collected}
    count_by_cat = {r["category"]: r["count"] for r in collected}

    result = {
        "pipeline": "DataFrame",
        "elapsed_seconds": round(elapsed, 4),
        "n_categories": len(avg_by_cat),
        "avg_normalized_by_category": avg_by_cat,
        "total_normalized_by_category": total_by_cat,
        "count_by_category": count_by_cat,
    }

    spark.sparkContext.parallelize([json.dumps(result)]).saveAsTextFile(output_path)
    print(json.dumps({"pipeline": "DataFrame", "elapsed_seconds": result["elapsed_seconds"]}))

    spark.stop()
    return result


if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "data/preprocessed_transactions.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output/dataframe_result"
    run(input_path, output_path)
