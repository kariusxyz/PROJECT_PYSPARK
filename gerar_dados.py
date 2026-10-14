from __future__ import annotations
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add, expr, lit, when

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"

def main():
    ap = argparse.ArgumentParser(description="Gera o dataset didático da Semana 09")
    ap.add_argument("--linhas", type=int, default=1_200_000, help="Quantidade de pedidos (padrão: 1.200.000)")
    args = ap.parse_args()
    n = max(10_000, args.linhas)

    spark = (SparkSession.builder.master("local[*]").appName("S09_GerarDados")
             .config("spark.sql.shuffle.partitions", "16").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("Spark UI:", spark.sparkContext.uiWebUrl or "indisponível")
    print(f"Gerando {n:,} pedidos...")

    ids = spark.range(1, n + 1).withColumnRenamed("id", "order_id")
    pedidos = (ids
      .withColumn("customer_id", when((col("order_id") % 100) < 35, lit(1)).otherwise((col("order_id") % 50_000) + 2))
      .withColumn("category_id", ((col("order_id") * 17) % 80 + 1).cast("int"))
      .withColumn("state_id", ((col("order_id") * 11) % 27 + 1).cast("int"))
      .withColumn("status", when((col("order_id") % 10) < 6, lit("paid")).when((col("order_id") % 10) < 8, lit("pending")).otherwise(lit("cancelled")))
      .withColumn("total", (((col("order_id") * 37) % 250_000) / 100.0 + 10.0).cast("double"))
      .withColumn("order_date", date_add(lit("2024-01-01").cast("date"), (col("order_id") % 730).cast("int"))))

    categorias = (spark.range(1,81).select(col("id").cast("int").alias("category_id"))
                  .withColumn("category_name", expr("concat('Categoria_', lpad(cast(category_id as string),2,'0'))")))
    estados = (spark.range(1,28).select(col("id").cast("int").alias("state_id"))
               .withColumn("state_name", expr("concat('UF_', lpad(cast(state_id as string),2,'0'))")))

    for name, df in [("pedidos",pedidos),("categorias",categorias),("estados",estados)]:
        path = str(DATA / name)
        df.write.mode("overwrite").parquet(path)
        print(f"OK: data/{name}/")

    print("\nDataset criado ✅")
    print("Observação didática: customer_id=1 concentra ~35% dos pedidos para demonstrar data skew.")
    spark.stop()

if __name__ == "__main__":
    main()
