from __future__ import annotations
from pathlib import Path
from time import perf_counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, count, desc, round, sum

BASE = Path(__file__).resolve().parent
DATA, OUT = BASE / "data", BASE / "output" / "otimizado"

spark = (SparkSession.builder.master("local[*]").appName("S09_Pipeline_Desafio")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

pedidos = spark.read.parquet(str(DATA/"pedidos"))
categorias = spark.read.parquet(str(DATA/"categorias"))
estados = spark.read.parquet(str(DATA/"estados"))

def filtrar_antes(df):
    # TODO 1 — aplique status=paid, total>0 e order_date>=2024-07-01
    return None

def joins_otimizados(df_filtrado, dim_categorias, dim_estados):
    # TODO 2 — faça LEFT JOIN usando broadcast() nas duas dimensões pequenas
    return None

def agregar_e_preparar_saida(df_completo):
    # TODO 3 — groupBy category_name/state_name; receita/pedidos/ticket_medio;
    # ordene por receita desc e reduza a saída para 4 partições com coalesce(4)
    return None

filtrados = filtrar_antes(pedidos)
if filtrados is None:
    print("\nPRÓXIMO PASSO: complete o TODO 1 em filtrar_antes(df).")
    spark.stop(); raise SystemExit(0)
print("Filtro antecipado OK. Linhas restantes:", filtrados.count())

completo = joins_otimizados(filtrados, categorias, estados)
if completo is None:
    print("\nPRÓXIMO PASSO: complete o TODO 2 em joins_otimizados(...).")
    spark.stop(); raise SystemExit(0)
print("Joins otimizados definidos.")

resultado = agregar_e_preparar_saida(completo)
if resultado is None:
    print("\nPRÓXIMO PASSO: complete o TODO 3 em agregar_e_preparar_saida(...).")
    spark.stop(); raise SystemExit(0)

print("\n=== PLANO DO PIPELINE OTIMIZADO ===")
resultado.explain("formatted")
print("\nSpark UI:", spark.sparkContext.uiWebUrl or "indisponível")
input("Abra a Spark UI e pressione ENTER para executar a escrita... ")

t0 = perf_counter()
resultado.write.mode("overwrite").parquet(str(OUT))
seg = perf_counter()-t0
print(f"\nPIPELINE OTIMIZADO concluído em {seg:.2f}s ✅")
print("Saída:", OUT)
print("Compare com o baseline na MESMA máquina.")
input("Registre a UI e pressione ENTER para encerrar... ")
spark.stop()
