# %% PREPARACION
import os
import platform
import numpy as np
import modin.config
modin.config.Engine.put("dask")
import modin.pandas as pd
import pandas as native_pd
from comun import (ENTRADA, ETIQUETAS, PROTOCOLOS, medir, cerrar, filas_tabla)

MOTOR = "modin"
TIEMPOS = []
df = pd.read_csv(ENTRADA)
# Comparación textual exacta para Q1/Q2; los cálculos numéricos usan df.
df_texto = pd.read_csv(ENTRADA, dtype=str, keep_default_na=False)
TOTAL = len(df)
assert TOTAL == 600_000 and len(df.columns) == 40
print("Motor:", MOTOR, "| backend:", modin.config.Engine.get(), "|", platform.platform())
print("Entrada:", ENTRADA, "| registros:", TOTAL)


def familia(datos):
    return datos.assign(Attack_Family=datos["Label"].map(ETIQUETAS))


def tipo(datos):
    return datos.assign(Traffic_Type=datos["Label"].map(
        lambda etiqueta: "Benign" if etiqueta == "Benign" else "Malicious"))


def limpio_rate(datos):
    # +inf -> NaN en Rate solamente: IAT retiene las 600k filas.
    return datos.assign(Rate=datos["Rate"].replace([np.inf, -np.inf], np.nan))


# %% Q1
def q01():
    nulos = int(df.isna().sum().sum())
    nums = df.select_dtypes(include="number")
    infs = int(np.isinf(nums.to_numpy()).sum())
    despues = len(df_texto.drop_duplicates())
    return [{"registros": TOTAL, "columnas": len(df.columns), "nulos": nulos,
             "infinitos": infs, "duplicados": TOTAL - despues}]


r01 = medir("Q1", q01, TIEMPOS, MOTOR)

# %% Q2
def q02():
    sin_duplicados = df_texto.drop_duplicates()  # no afecta al original
    despues = len(sin_duplicados)
    return [{"antes": TOTAL, "despues": despues,
             "eliminados": TOTAL - despues,
             "reduccion_pct": (TOTAL - despues) * 100 / TOTAL}]


r02 = medir("Q2", q02, TIEMPOS, MOTOR)

# %% Q3
def q03():
    global clasificado
    clasificado = familia(df)
    t = clasificado.groupby("Attack_Family").agg(
        cantidad=("Label", "size"), clases=("Label", "nunique")).reset_index()
    assert int(t["cantidad"].sum()) == TOTAL and int(t["clases"].sum()) == 34
    assert int(df["Label"].nunique()) == 34
    return filas_tabla(t.sort_values("Attack_Family"))


r03 = medir("Q3", q03, TIEMPOS, MOTOR)

# %% Q4
def q04():
    n = len(df[df["Label"] != "Benign"])
    return [{"cantidad": n, "porcentaje": n * 100 / TOTAL}]


r04 = medir("Q4", q04, TIEMPOS, MOTOR)

# %% Q5
def q05():
    global tipado
    tipado = tipo(df)
    t = tipado.groupby("Traffic_Type").size().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values("Traffic_Type"))


r05 = medir("Q5", q05, TIEMPOS, MOTOR)

# %% Q6
def q06():
    t = clasificado.groupby("Attack_Family").size().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values(["cantidad", "Attack_Family"], ascending=[False, True]))


r06 = medir("Q6", q06, TIEMPOS, MOTOR)

# %% Q7
def q07():
    t = df[df["Label"] != "Benign"].groupby("Label").size().rename("cantidad").reset_index()
    t["porcentaje"] = t["cantidad"] * 100 / TOTAL
    return filas_tabla(t.sort_values(["cantidad", "Label"], ascending=[False, True]).head(10))


r07 = medir("Q7", q07, TIEMPOS, MOTOR)

# %% Q8
def q08():
    limpio = limpio_rate(clasificado)
    t = limpio.groupby("Attack_Family").agg(
        registros=("IAT", "size"), validos_rate=("Rate", "count"),
        rate_media=("Rate", "mean"), rate_mediana=("Rate", "median"),
        rate_min=("Rate", "min"), rate_max=("Rate", "max"),
        iat_media=("IAT", "mean"), iat_mediana=("IAT", "median"),
        iat_min=("IAT", "min"), iat_max=("IAT", "max")).reset_index()
    t["rate_inf_excluidos"] = t["registros"] - t["validos_rate"]
    cols=["Attack_Family", "registros", "rate_inf_excluidos", "rate_media", "rate_mediana",
          "rate_min", "rate_max", "iat_media", "iat_mediana", "iat_min", "iat_max"]
    assert int(t["rate_inf_excluidos"].sum()) == 13
    return filas_tabla(t.sort_values("Attack_Family")[cols])


r08 = medir("Q8", q08, TIEMPOS, MOTOR)

# %% Q9
def q09():
    t = clasificado.assign(protocolo=clasificado["Protocol Type"].map(PROTOCOLOS))
    t = t.groupby(["Attack_Family", "Protocol Type", "protocolo"]).size().rename("cantidad").reset_index()
    # 8 filas en el driver para los denominadores, no para los 600k registros.
    total = t.groupby("Attack_Family")["cantidad"].sum().to_dict()
    t["total_familia"] = t["Attack_Family"].map(total)
    t["porcentaje_familia"] = 100 * t["cantidad"] / t["total_familia"]
    return filas_tabla(t.sort_values(["Attack_Family", "Protocol Type"]))


r09 = medir("Q9", q09, TIEMPOS, MOTOR)

# %% Q10
def q10():
    limpio = limpio_rate(tipado)
    t = limpio.groupby("Traffic_Type").agg(
        registros=("IAT", "size"), validos_rate=("Rate", "count"),
        rate_media=("Rate", "mean"), rate_mediana=("Rate", "median"),
        iat_media=("IAT", "mean"), iat_mediana=("IAT", "median"),
        ack_flag_media=("ack_flag_number", "mean"),
        syn_flag_media=("syn_flag_number", "mean"),
        tot_sum_media=("Tot sum", "mean"), avg_media=("AVG", "mean")).reset_index()
    t["rate_inf_excluidos"] = t["registros"] - t["validos_rate"]
    cols=["Traffic_Type", "registros", "rate_inf_excluidos", "rate_media",
          "rate_mediana", "iat_media", "iat_mediana", "ack_flag_media",
          "syn_flag_media", "tot_sum_media", "avg_media"]
    assert int(t["rate_inf_excluidos"].sum()) == 13
    return filas_tabla(t.sort_values("Traffic_Type")[cols])


r10 = medir("Q10", q10, TIEMPOS, MOTOR)

# %% CIERRE
cerrar(MOTOR, TIEMPOS)
