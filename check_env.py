from __future__ import annotations
import platform, re, subprocess, sys

def java_version():
    try:
        p = subprocess.run(["java", "-version"], capture_output=True, text=True, timeout=10)
        txt = (p.stderr or p.stdout).splitlines()[0]
        m = re.search(r'"(\d+)(?:\.(\d+))?', txt)
        return txt, int(m.group(1)) if m else None
    except Exception as exc:
        return f"ERRO: {exc}", None

print("=== CHECK DE AMBIENTE · SEMANA 09 ===")
print("Python:", sys.version.split()[0], "|", platform.system())
py_ok = sys.version_info >= (3, 10)
print("Python 3.10+:", "OK" if py_ok else "AJUSTAR")
java_txt, java_major = java_version()
print("Java:", java_txt)
java_ok = java_major is not None and java_major >= 17
print("Java 17+:", "OK" if java_ok else "AJUSTAR")

try:
    import pyspark
    print("PySpark:", pyspark.__version__)
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]").appName("S09_Check").getOrCreate())
    print("Spark:", spark.version)
    print("Spark UI:", spark.sparkContext.uiWebUrl or "não disponível")
    spark.range(10).count()
    spark.stop()
    spark_ok = True
except Exception as exc:
    spark_ok = False
    print("PySpark/Spark: ERRO")
    print(type(exc).__name__ + ":", exc)

if py_ok and java_ok and spark_ok:
    print("\nAMBIENTE OK ✅")
else:
    print("\nAMBIENTE PRECISA DE AJUSTE ⚠")
    sys.exit(1)
