"""
generate_dataset.py
---------------------------------------------------------------------
Genera el dataset preprocesado (salida simulada de la Etapa 1 / GPU)
que la Etapa 2 (Spark) consume desde HDFS/S3. Simula transacciones de
comercios dominicanos (MiPymes) con montos ya normalizados [0,1] y
categorías, para poder calcular agregaciones (promedio por categoría,
total, conteo) tanto en RDD como en DataFrame.
"""
import csv
import random
import sys

random.seed(42)

CATEGORIES = ["alimentos", "ferreteria", "farmacia", "ropa", "electronica",
              "servicios", "transporte", "restaurante", "colmado", "otros"]

def generate(n_rows: int, out_path: str):
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "merchant_id", "category", "amount_normalized", "raw_amount"])
        for i in range(n_rows):
            raw = round(random.uniform(50, 25000), 2)  # DOP
            norm = round(raw / 25000.0, 6)
            writer.writerow([
                f"tx-{i:09d}",
                f"merchant-{random.randint(1, 5000):05d}",
                random.choice(CATEGORIES),
                norm,
                raw,
            ])

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 1_000_000
    out = sys.argv[2] if len(sys.argv) > 2 else "preprocessed_transactions.csv"
    generate(n, out)
    print(f"Generadas {n} filas en {out}")
