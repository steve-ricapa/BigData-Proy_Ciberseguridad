# %% PREPARACION
import csv
import os
import platform
import sys
from pyspark.sql import SparkSession, functions as F, types as T
from comun import (ENTRADA, ETIQUETAS, PROTOCOLOS, medir, cerrar, filas_tabla)

os.environ["PYSPARK_PYTHON"] = sys.executable
MOTOR = "spark"
TIEMPOS = []
with ENTRADA.open(encoding="utf-8", newline="") as entrada:
    COLUMNAS = next(csv.reader(entrada))
ENTERAS = {"Protocol Type", "ack_count", "syn_count", "fin_count", "rst_count",
           "Tot sum", "Min", "Max", "Number"}
ESQUEMA = T.StructType([T.StructField(c, T.StringType() if c in ("Label", "Rate")
                                      else T.LongType() if c in ENTERAS else T.DoubleType(), True)
                         for c in COLUMNAS])
spark = (SparkSession.builder.master("local[4]").appName("fase6-ciciot2023")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "4g")
         .config("spark.driver.maxResultSize", "256m")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
# `Rate` como texto al leer: Spark interpreta "inf" como null si se fuerza DoubleType.
# Cast explícito preserva los 13 infinitos y permite contarlos en Q1.
df = (spark.read.option("header", True).schema(ESQUEMA).csv(str(ENTRADA))
      .withColumn("Rate", F.when(F.lower(F.col("Rate")) == "inf", F.lit(float("inf")))
                  .when(F.lower(F.col("Rate")) == "-inf", F.lit(float("-inf")))
                  .otherwise(F.col("Rate").cast("double"))).cache())
TOTAL = df.count()
assert TOTAL == 600_000 and len(df.columns) == 40
print("Motor:", MOTOR, spark.version, "| master:", spark.sparkContext.master, "|", platform.platform())
print("Entrada:", ENTRADA, "| registros:", TOTAL)


def familia(datos):
    m = F.create_map(*[v for clase, familia_ in ETIQUETAS.items()
                       for v in (F.lit(clase), F.lit(familia_))])
    return datos.withColumn("Attack_Family", m[F.col("Label")])


def tipo(datos):
    return datos.withColumn("Traffic_Type", F.when(F.col("Label") == "Benign", "Benign")
                           .otherwise("Malicious"))


def limpio_rate(datos):
    # Se excluyen SOLO los 13 Rate=inf de agregaciones de Rate.
    # IAT, AVG, Tot sum y flags retienen todas las filas.
    return datos.withColumn("Rate", F.when(F.abs("Rate") == F.lit(float("inf")),
                                   F.lit(None).cast("double")).otherwise(F.col("Rate")))


def resumen_rate_iat(datos):
    return [F.count("*").alias("registros"),
            (F.count("*") - F.count("Rate")).alias("rate_inf_excluidos"),
            F.avg("Rate").alias("rate_media"),
            F.percentile("Rate", F.lit(0.5)).alias("rate_mediana"),
            F.min("Rate").alias("rate_min"), F.max("Rate").alias("rate_max"),
            F.avg("IAT").alias("iat_media"),
            F.percentile("IAT", F.lit(0.5)).alias("iat_mediana"),
            F.min("IAT").alias("iat_min"), F.max("IAT").alias("iat_max")]


# %% Q1
def q01():
    # Solo 2 columnas contienen nulos en esta muestra; comprobar TODAS igualmente.
    nulos = df.agg(*[F.sum(F.col(f"`{c}`").isNull().cast("long")).alias(c)
                      for c in COLUMNAS]).first().asDict()
    flotantes = [c for c in COLUMNAS if c not in ENTERAS | {"Label"}]
    infs = df.agg(*[F.sum((F.abs(F.col(f"`{c}`")) == F.lit(float("inf"))).cast("long"))
                     .alias(c) for c in flotantes]).first().asDict()
    despues = df.dropDuplicates().count()
    return [{"registros": TOTAL, "columnas": len(COLUMNAS),
             "nulos": sum(nulos.values()), "infinitos": sum(infs.values()),
             "duplicados": TOTAL - despues}]


r01 = medir("Q1", q01, TIEMPOS, MOTOR)

# %% Q2
def q02():
    sin_duplicados = df.dropDuplicates()
    despues = sin_duplicados.count()
    return [{"antes": TOTAL, "despues": despues,
             "eliminados": TOTAL - despues,
             "reduccion_pct": (TOTAL - despues) * 100 / TOTAL}]


r02 = medir("Q2", q02, TIEMPOS, MOTOR)

# %% Q3
def q03():
    global clasificado
    clasificado = familia(df).cache()
    t = clasificado.groupBy("Attack_Family").agg(
        F.count("*").alias("cantidad"), F.countDistinct("Label").alias("clases"))
    resultado = filas_tabla(t.orderBy("Attack_Family"))
    assert all(r["Attack_Family"] in set(ETIQUETAS.values()) for r in resultado)
    assert sum(r["clases"] for r in resultado) == 34
    assert sum(r["cantidad"] for r in resultado) == TOTAL
    return resultado


r03 = medir("Q3", q03, TIEMPOS, MOTOR)

# %% Q4
def q04():
    n = df.filter(F.col("Label") != "Benign").count()
    return [{"cantidad": n, "porcentaje": 100 * n / TOTAL}]


r04 = medir("Q4", q04, TIEMPOS, MOTOR)

# %% Q5
def q05():
    global tipado
    tipado = tipo(df).cache()
    t = tipado.groupBy("Traffic_Type").agg(F.count("*").alias("cantidad"))
    t = t.withColumn("porcentaje", 100 * F.col("cantidad") / F.lit(TOTAL))
    return filas_tabla(t.orderBy("Traffic_Type"))


r05 = medir("Q5", q05, TIEMPOS, MOTOR)

# %% Q6
def q06():
    t = clasificado.groupBy("Attack_Family").agg(F.count("*").alias("cantidad"))
    t = t.withColumn("porcentaje", 100 * F.col("cantidad") / F.lit(TOTAL))
    return filas_tabla(t.orderBy(F.desc("cantidad"), "Attack_Family"))


r06 = medir("Q6", q06, TIEMPOS, MOTOR)

# %% Q7
def q07():
    t = df.filter(F.col("Label") != "Benign").groupBy("Label").agg(
        F.count("*").alias("cantidad"))
    t = t.withColumn("porcentaje", 100 * F.col("cantidad") / F.lit(TOTAL))
    return filas_tabla(t.orderBy(F.desc("cantidad"), "Label").limit(10))


r07 = medir("Q7", q07, TIEMPOS, MOTOR)

# %% Q8
def q08():
    t = limpio_rate(clasificado).groupBy("Attack_Family").agg(
        *resumen_rate_iat(clasificado))
    resultado = filas_tabla(t.orderBy("Attack_Family"))
    assert sum(r["rate_inf_excluidos"] for r in resultado) == 13
    return resultado


r08 = medir("Q8", q08, TIEMPOS, MOTOR)

# %% Q9
def q09():
    mapa = F.create_map(*[v for num, nombre in PROTOCOLOS.items()
                          for v in (F.lit(num), F.lit(nombre))])
    d = clasificado.withColumn("protocolo", mapa[F.col("Protocol Type")])
    t = d.groupBy("Attack_Family", "Protocol Type", "protocolo").agg(
        F.count("*").alias("cantidad"))
    total = clasificado.groupBy("Attack_Family").agg(
        F.count("*").alias("total_familia"))
    t = t.join(total, "Attack_Family").withColumn(
        "porcentaje_familia", 100 * F.col("cantidad") / F.col("total_familia"))
    return filas_tabla(t.orderBy("Attack_Family", "Protocol Type"))


r09 = medir("Q9", q09, TIEMPOS, MOTOR)

# %% Q10
def q10():
    t = limpio_rate(tipado).groupBy("Traffic_Type").agg(
        F.count("*").alias("registros"),
        (F.count("*") - F.count("Rate")).alias("rate_inf_excluidos"),
        F.avg("Rate").alias("rate_media"),
        F.percentile("Rate", F.lit(0.5)).alias("rate_mediana"),
        F.avg("IAT").alias("iat_media"),
        F.percentile("IAT", F.lit(0.5)).alias("iat_mediana"),
        F.avg("ack_flag_number").alias("ack_flag_media"),
        F.avg("syn_flag_number").alias("syn_flag_media"),
        F.avg("Tot sum").alias("tot_sum_media"),
        F.avg("AVG").alias("avg_media"))
    resultado = filas_tabla(t.orderBy("Traffic_Type"))
    assert sum(r["rate_inf_excluidos"] for r in resultado) == 13
    return resultado


r10 = medir("Q10", q10, TIEMPOS, MOTOR)

# %% CIERRE
cerrar(MOTOR, TIEMPOS)
spark.stop()
