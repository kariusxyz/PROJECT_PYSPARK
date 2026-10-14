from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, spark_partition_id

BASE=Path(__file__).resolve().parent
spark=(SparkSession.builder.master("local[*]").appName("S09_Skew_Demo").config("spark.sql.shuffle.partitions","16").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
pedidos=spark.read.parquet(str(BASE/"data"/"pedidos"))
print("\nTop 10 customer_id por volume:")
pedidos.groupBy("customer_id").count().orderBy(desc("count")).show(10,truncate=False)
print("\nDistribuição após repartition(16, 'customer_id'):")
part=(pedidos.repartition(16,"customer_id").withColumn("partition_id",spark_partition_id()).groupBy("partition_id").count().orderBy("partition_id"))
part.show(16,truncate=False)
print("Spark UI:", spark.sparkContext.uiWebUrl or "indisponível")
input("Observe a diferença entre partições e pressione ENTER para encerrar... ")
spark.stop()
