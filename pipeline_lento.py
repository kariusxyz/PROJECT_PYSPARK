from __future__ import annotations
from pathlib import Path
from time import perf_counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, round, sum

BASE = Path(__file__).resolve().parent
DATA, OUT = BASE / "data", BASE / "output" / "baseline_lento"

spark = (SparkSession.builder.master("local[*]").appName("S09_Pipeline_Lento")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pedidos = spark.read.parquet(str(DATA/"pedidos"))
categorias = spark.read.parquet(str(DATA/"categorias"))
estados = spark.read.parquet(str(DATA/"estados"))

# DIDÁTICO: joins antes do filtro + auto-broadcast desligado.
completo = (pedidos
    .join(categorias, "category_id", "left")
    .join(estados, "state_id", "left"))

validos = (completo
    .filter(col("status") == "paid")
    .filter(col("total") > 0)
    .filter(col("order_date") >= "2024-07-01"))

resultado = (validos
    .groupBy("category_name", "state_name")
    .agg(round(sum("total"),2).alias("receita"), count("order_id").alias("pedidos"), round(avg("total"),2).alias("ticket_medio"))
    .orderBy(desc("receita")))

print("\n=== PLANO DO PIPELINE LENTO ===")
resultado.explain("formatted")
print("\nSpark UI:", spark.sparkContext.uiWebUrl or "indisponível")
input("Abra a Spark UI e pressione ENTER para iniciar a action... ")

t0 = perf_counter()
resultado.write.mode("overwrite").parquet(str(OUT))
seg = perf_counter()-t0
print(f"\nBASELINE concluído em {seg:.2f}s")
print("Saída:", OUT)
print("Registre: estratégia dos joins, Shuffle Read/Write e duração das tasks.")
input("Com a Spark UI aberta, registre suas evidências. Pressione ENTER para encerrar... ")
spark.stop()
