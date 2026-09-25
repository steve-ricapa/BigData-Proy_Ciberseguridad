"""Configuracion comun de la Fase 4: rutas, columnas y parametros de comparacion.

Este modulo no importa ningun motor de procesamiento. Solo define el contrato
compartido por Polars, Dask, Modin y Spark para que las 10 operaciones
produzcan resultados comparables.
"""

from __future__ import annotations

import json
from pathlib import Path

# --- Rutas -------------------------------------------------------------------

FASE = Path(__file__).resolve().parents[1]
RAIZ = FASE.parent

CSV_MUESTRA = RAIZ / "01_fase1_datos" / "muestra" / "CICIoT2023_sample_600k.csv"
RUTA_UMBRALES = FASE / "comun" / "umbrales.json"
RUTA_RESULTADOS = RAIZ / "results" / "fase4"
RUTA_REPORTS = RAIZ / "reports"

# --- Expectativas del dataset (validadas en la Fase 1) -----------------------

TOTAL_FILAS_ESPERADAS = 600_000
TOTAL_COLUMNAS_ESPERADAS = 40
TOTAL_ETIQUETAS_ESPERADAS = 34
ETIQUETA_BENIGNA = "Benign"

# --- Estructura del dataset --------------------------------------------------

COLS_FLAGS = [
    "fin_flag_number",
    "syn_flag_number",
    "rst_flag_number",
    "psh_flag_number",
    "ack_flag_number",
    "ece_flag_number",
    "cwr_flag_number",
]

COLS_CONTADORES = ["ack_count", "syn_count", "fin_count", "rst_count"]

COLS_PROTOCOLO = [
    "HTTP",
    "HTTPS",
    "DNS",
    "Telnet",
    "SMTP",
    "SSH",
    "IRC",
    "TCP",
    "UDP",
    "DHCP",
    "ARP",
    "ICMP",
    "IGMP",
    "IPv",
    "LLC",
]

# Columnas numericas usadas para metricas, agregaciones y transformaciones.
COLS_INDICADORES = [
    "Rate",
    "Tot size",
    "Tot sum",
    "IAT",
    "Number",
    "Variance",
    "AVG",
    "Std",
    "Min",
    "Max",
]

# Subconjunto de claves numericas de alta cardinalidad (evita duplicados exactos).
COLS_CLAVE_DUPLICADOS = ["Label", "Protocol Type", "Tot size", "IAT", "Rate", "Number"]

# Columnas cuyo numero de valores distintos se calcula (exactamente).
# El resto se reporta como vacio para mantener el costo acotado en Spark.
COLS_DISTINTOS = (
    ["Label", "Protocol Type"]
    + COLS_PROTOCOLO
    + COLS_FLAGS
    + ["Rate", "Tot size", "Number", "Variance"]
)

COLUMNAS = [
    "Header_Length",
    "Protocol Type",
    "Time_To_Live",
    "Rate",
] + COLS_FLAGS + COLS_CONTADORES + COLS_PROTOCOLO + [
    "Tot sum",
    "Min",
    "Max",
    "AVG",
    "Std",
    "Tot size",
    "IAT",
    "Number",
    "Variance",
    "Label",
]

# --- Parametros de las operaciones -------------------------------------------

DECIMALES = 6
FILAS_MUESTRA_TRANSFORMACION = 1_000
TOP_N = 10

# La muestra contiene 13 valores "inf" en la columna Rate y 18 celdas vacias en
# Std y Variance. Todos los motores convierten un valor no finito (inf, -inf o
# NaN) en nulo y luego eliminan la fila, de modo que la limpieza sea identica.
TRATAR_INFINITOS = True

# Cada motor usa un parser de CSV distinto y los ultimos decimales de un float
# pueden diferir en torno a 1e-11. Redondear al cargar garantiza que los cuatro
# motores trabajen exactamente sobre los mismos valores y que la comparacion de
# duplicados y agregaciones sea reproducible.
REDONDEO_INGESTA = 6


def cargar_umbrales() -> dict:
    """Lee los percentiles calculados una sola vez por `generar_umbrales.py`.

    Devuelve el diccionario interno con una entrada por columna, por ejemplo
    ``umbrales["Rate"]["p95"]``.

    Los umbrales se calculan con la biblioteca estandar para que los cuatro
    motores apliquen exactamente el mismo filtro.
    """
    with open(RUTA_UMBRALES, encoding="utf-8") as archivo:
        return json.load(archivo)["umbrales"]
