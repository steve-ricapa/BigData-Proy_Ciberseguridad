# %% PREPARACION
import os
import platform
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
from comun import (ENTRADA, ETIQUETAS, PROTOCOLOS, medir, cerrar, filas_tabla)

MOTOR = "dask"
TIEMPOS = []
dask.config.set(scheduler="threads", num_workers=4)
df = dd.read_csv(ENTRADA, blocksize="16MB", sample=256_000,
                 dtype={"Label": "str"})
# Lectura adicional solo para deduplicación exacta: evita que la conversión
# decimal del parser fusione accidentalmente dos registros distintos.
df_texto = dd.read_csv(ENTRADA, blocksize="16MB", dtype="str", keep_default_na=False)
TOTAL = int(df.map_partitions(len).sum().compute())
assert TOTAL == 600_000 and len(df.columns) == 40
print("Motor:", MOTOR, dask.__version__, "| scheduler: threads 4 |", platform.platform())
print("Entrada:", ENTRADA, "| particiones:", df.npartitions, "| registros:", TOTAL)


def familia(datos):
    return datos.assign(Attack_Family=datos["Label"].map(ETIQUETAS,
                              meta=("Attack_Family", "object")))


def tipo(datos):
    return datos.assign(Traffic_Type=datos["Label"].map(
        lambda s: "Benign" if s == "Benign" else "Malicious",
        meta=("Traffic_Type", "object")))


def limpio_rate(datos):
    return datos.assign(Rate=datos["Rate"].replace([np.inf, -np.inf], np.nan))


def mediana_por_grupo(datos, grupo, cols):
    """Medianas exactas: shuffle por grupo, cálculo pandas dentro de cada grupo."""
    def fn(pdf):
        return pd.Series({f"{c}_mediana": pdf[c].median() for c in cols})
    meta=pd.DataFrame({f"{c}_mediana": pd.Series(dtype="float64") for c in cols})
    return datos.groupby(grupo).apply(fn, meta=meta, include_groups=False).compute().reset_index()


# %% Q1
def q01():
    nulos = int(df.isna().sum().sum().compute())
    nums = df.select_dtypes(include="number")
    infs = int(nums.map_partitions(lambda pdf: pd.Series([np.isinf(pdf.to_numpy()).sum()]),
                                   meta=pd.Series(dtype="int64")).sum().compute())
    distintos = int(df_texto.drop_duplicates().map_partitions(len).sum().compute())
    return [{"registros": TOTAL, "columnas": len(df.columns), "nulos": nulos,
             "infinitos": infs, "duplicados": TOTAL - distintos}]


r01 = medir("Q1", q01, TIEMPOS, MOTOR)

# %% Q2
def q02():
    sin_duplicados = df_texto.drop_duplicates()  # comparación exacta del CSV
    despues = int(sin_duplicados.map_partitions(len).sum().compute())
    return [{"antes": TOTAL, "despues": despues,
             "eliminados": TOTAL - despues,
             "reduccion_pct": 100 * (TOTAL - despues) / TOTAL}]


r02 = medir("Q2", q02, TIEMPOS, MOTOR)

# %% Q3
def q03():
    global clasificado
    clasificado = familia(df)
    cantidades = clasificado.groupby("Attack_Family").size().compute().rename("cantidad").reset_index()
    clases = (clasificado[["Label", "Attack_Family"]].drop_duplicates()
              .groupby("Attack_Family").size().compute().rename("clases").reset_index())
    t = cantidades.merge(clases, on="Attack_Family").sort_values("Attack_Family")
    assert int(t["cantidad"].sum()) == TOTAL and int(t["clases"].sum()) == 34
    assert len(df["Label"].unique().compute()) == 34
    return filas_tabla(t)


r03 = medir("Q3", q03, TIEMPOS, MOTOR)

# %% Q4
def q04():
    n = int(df[df["Label"] != "Benign"].map_partitions(len).sum().compute())
    return [{"cantidad": n, "porcentaje": n * 100 / TOTAL}]


r04 = medir("Q4", q04, TIEMPOS, MOTOR)

# %% Q5
def q05():
    global tipado
    tipado = tipo(df)
    t = tipado.groupby("Traffic_Type").size().compute().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values("Traffic_Type"))


r05 = medir("Q5", q05, TIEMPOS, MOTOR)

# %% Q6
def q06():
    t = clasificado.groupby("Attack_Family").size().compute().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values(["cantidad", "Attack_Family"], ascending=[False, True]))


r06 = medir("Q6", q06, TIEMPOS, MOTOR)

# %% Q7
def q07():
    t = df[df["Label"] != "Benign"].groupby("Label").size().compute().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values(["cantidad", "Label"], ascending=[False, True]).head(10))


r07 = medir("Q7", q07, TIEMPOS, MOTOR)

# %% Q8
def q08():
    limpio = limpio_rate(clasificado)
    base = limpio.groupby("Attack_Family").agg({
        "Rate": ["mean", "min", "max", "count"],
        "IAT": ["mean", "min", "max", "count"]}).compute()
    base.columns = ["rate_media", "rate_min", "rate_max", "validos_rate",
                    "iat_media", "iat_min", "iat_max", "registros"]
    base = base.reset_index()
    base["rate_inf_excluidos"] = base["registros"] - base["validos_rate"]
    med = mediana_por_grupo(limpio, "Attack_Family", ["Rate", "IAT"])
    t = base.merge(med, on="Attack_Family")
    t = t.rename(columns={"Rate_mediana": "rate_mediana", "IAT_mediana": "iat_mediana"})
    cols=["Attack_Family", "registros", "rate_inf_excluidos", "rate_media", "rate_mediana",
          "rate_min", "rate_max", "iat_media", "iat_mediana", "iat_min", "iat_max"]
    assert int(t["rate_inf_excluidos"].sum()) == 13
    return filas_tabla(t.sort_values("Attack_Family")[cols])


r08 = medir("Q8", q08, TIEMPOS, MOTOR)

# %% Q9
def q09():
    d = clasificado.assign(protocolo=clasificado["Protocol Type"].map(
        PROTOCOLOS, meta=("protocolo", "object")))
    t = d.groupby(["Attack_Family", "Protocol Type", "protocolo"]).size().compute().rename("cantidad").reset_index()
    total = t.groupby("Attack_Family")["cantidad"].transform("sum")
    t["total_familia"] = total
    t["porcentaje_familia"] = 100 * t["cantidad"] / total
    return filas_tabla(t.sort_values(["Attack_Family", "Protocol Type"]))


r09 = medir("Q9", q09, TIEMPOS, MOTOR)

# %% Q10
def q10():
    limpio = limpio_rate(tipado)
    base = limpio.groupby("Traffic_Type").agg({
        "Rate": ["mean", "count"], "IAT": "mean", "ack_flag_number": "mean",
        "syn_flag_number": "mean", "Tot sum": "mean", "AVG": "mean"}).compute()
    base.columns = ["rate_media", "validos_rate", "iat_media", "ack_flag_media",
                    "syn_flag_media", "tot_sum_media", "avg_media"]
    base = base.reset_index()
    cuenta = limpio.groupby("Traffic_Type").size().compute().rename("registros").reset_index()
    base = base.merge(cuenta, on="Traffic_Type")
    base["rate_inf_excluidos"] = base["registros"] - base["validos_rate"]
    med = mediana_por_grupo(limpio, "Traffic_Type", ["Rate", "IAT"])
    t = base.merge(med, on="Traffic_Type").rename(columns={
        "Rate_mediana": "rate_mediana", "IAT_mediana": "iat_mediana"})
    cols=["Traffic_Type", "registros", "rate_inf_excluidos", "rate_media",
          "rate_mediana", "iat_media", "iat_mediana", "ack_flag_media",
          "syn_flag_media", "tot_sum_media", "avg_media"]
    assert int(t["rate_inf_excluidos"].sum()) == 13
    return filas_tabla(t.sort_values("Traffic_Type")[cols])


r10 = medir("Q10", q10, TIEMPOS, MOTOR)

# %% CIERRE
cerrar(MOTOR, TIEMPOS)
