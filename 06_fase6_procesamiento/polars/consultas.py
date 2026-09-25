# %% PREPARACION
import os
import platform
import polars as pl
from comun import (ENTRADA, ETIQUETAS, PROTOCOLOS, FASE, medir, cerrar, filas_tabla)

MOTOR = "polars"
TIEMPOS = []
df = pl.read_csv(ENTRADA)
TOTAL = df.height
assert TOTAL == 600_000 and len(df.columns) == 40
print("Motor:", MOTOR, pl.__version__, "| entorno:", platform.platform(), "| CPU:", os.cpu_count())
print("Entrada:", ENTRADA, "| registros:", TOTAL)


def familia(df_):
    return df_.with_columns(pl.col("Label").replace_strict(ETIQUETAS).alias("Attack_Family"))


def tipo(df_):
    return df_.with_columns(pl.when(pl.col("Label") == "Benign")
                            .then(pl.lit("Benign")).otherwise(pl.lit("Malicious"))
                            .alias("Traffic_Type"))


def limpio_rate(df_):
    # NULL para los 13 +inf; no se eliminan filas ni se limpia IAT.
    return df_.with_columns(pl.when(pl.col("Rate").is_finite())
                            .then(pl.col("Rate")).otherwise(None).alias("Rate"))


# %% Q1
def q01():
    nulos = sum(df[c].null_count() for c in df.columns)
    infs = sum(int(df[c].is_infinite().sum()) for c, t in df.schema.items() if t.is_float())
    return [{"registros": TOTAL, "columnas": len(df.columns), "nulos": nulos,
             "infinitos": infs, "duplicados": TOTAL - df.unique().height}]


r01 = medir("Q1", q01, TIEMPOS, MOTOR)

# %% Q2
def q02():
    sin_duplicados = df.unique()  # versión lógica, NO sobrescribe el CSV.
    despues = sin_duplicados.height
    return [{"antes": TOTAL, "despues": despues,
             "eliminados": TOTAL - despues,
             "reduccion_pct": 100 * (TOTAL - despues) / TOTAL}]


r02 = medir("Q2", q02, TIEMPOS, MOTOR)

# %% Q3
def q03():
    global clasificado
    clasificado = familia(df)
    t = clasificado.group_by("Attack_Family").agg(
        pl.len().alias("cantidad"), pl.col("Label").n_unique().alias("clases"))
    assert clasificado.select(pl.col("Label").n_unique()).item() == 34
    assert t["clases"].sum() == 34 and t["cantidad"].sum() == TOTAL
    return filas_tabla(t.sort("Attack_Family"))


r03 = medir("Q3", q03, TIEMPOS, MOTOR)

# %% Q4
def q04():
    maliciosos = df.filter(pl.col("Label") != "Benign")
    return [{"cantidad": maliciosos.height, "porcentaje": 100 * maliciosos.height / TOTAL}]


r04 = medir("Q4", q04, TIEMPOS, MOTOR)

# %% Q5
def q05():
    global tipado
    tipado = tipo(df)
    t = tipado.group_by("Traffic_Type").agg(pl.len().alias("cantidad"))
    t = t.with_columns((pl.col("cantidad") * 100 / TOTAL).alias("porcentaje"))
    return filas_tabla(t.sort("Traffic_Type"))


r05 = medir("Q5", q05, TIEMPOS, MOTOR)

# %% Q6
def q06():
    t = clasificado.group_by("Attack_Family").agg(pl.len().alias("cantidad"))
    t = t.with_columns((pl.col("cantidad") * 100 / TOTAL).alias("porcentaje"))
    return filas_tabla(t.sort(["cantidad", "Attack_Family"], descending=[True, False]))


r06 = medir("Q6", q06, TIEMPOS, MOTOR)

# %% Q7
def q07():
    t = df.filter(pl.col("Label") != "Benign").group_by("Label").agg(
        pl.len().alias("cantidad"))
    t = t.with_columns((pl.col("cantidad") * 100 / TOTAL).alias("porcentaje"))
    return filas_tabla(t.sort(["cantidad", "Label"], descending=[True, False]).head(10))


r07 = medir("Q7", q07, TIEMPOS, MOTOR)

# %% Q8
def q08():
    # Rate=+inf -> NULL solo para esa columna: IAT incluye las 600k filas.
    t = limpio_rate(clasificado).group_by("Attack_Family").agg(
        pl.len().alias("registros"), pl.col("Rate").null_count().alias("rate_inf_excluidos"),
        pl.col("Rate").mean().alias("rate_media"),
        pl.col("Rate").median().alias("rate_mediana"),
        pl.col("Rate").min().alias("rate_min"), pl.col("Rate").max().alias("rate_max"),
        pl.col("IAT").mean().alias("iat_media"),
        pl.col("IAT").median().alias("iat_mediana"),
        pl.col("IAT").min().alias("iat_min"), pl.col("IAT").max().alias("iat_max"))
    assert t["rate_inf_excluidos"].sum() == 13
    return filas_tabla(t.sort("Attack_Family"))


r08 = medir("Q8", q08, TIEMPOS, MOTOR)

# %% Q9
def q09():
    t = clasificado.with_columns(pl.col("Protocol Type").replace_strict(PROTOCOLOS)
                                  .alias("protocolo"))
    t = t.group_by("Attack_Family", "Protocol Type", "protocolo").agg(
        pl.len().alias("cantidad"))
    totales = clasificado.group_by("Attack_Family").agg(pl.len().alias("total_familia"))
    t = t.join(totales, on="Attack_Family").with_columns(
        (100 * pl.col("cantidad") / pl.col("total_familia")).alias("porcentaje_familia"))
    return filas_tabla(t.sort(["Attack_Family", "Protocol Type"]))


r09 = medir("Q9", q09, TIEMPOS, MOTOR)

# %% Q10
def q10():
    t = limpio_rate(tipado).group_by("Traffic_Type").agg(
        pl.len().alias("registros"), pl.col("Rate").null_count().alias("rate_inf_excluidos"),
        pl.col("Rate").mean().alias("rate_media"),
        pl.col("Rate").median().alias("rate_mediana"),
        pl.col("IAT").mean().alias("iat_media"),
        pl.col("IAT").median().alias("iat_mediana"),
        pl.col("ack_flag_number").mean().alias("ack_flag_media"),
        pl.col("syn_flag_number").mean().alias("syn_flag_media"),
        pl.col("Tot sum").mean().alias("tot_sum_media"),
        pl.col("AVG").mean().alias("avg_media"))
    assert t["rate_inf_excluidos"].sum() == 13
    return filas_tabla(t.sort("Traffic_Type"))


r10 = medir("Q10", q10, TIEMPOS, MOTOR)

# %% CIERRE
cerrar(MOTOR, TIEMPOS)
