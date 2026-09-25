"""Calcula los percentiles usados por los filtros de la operacion 05.

Se hace una sola vez, con la biblioteca estandar, y el resultado se guarda en
comun/umbrales.json. Los cuatro motores leen ese archivo, de modo que el filtro
es exactamente el mismo en Polars, Dask, Modin y Spark.
"""

from __future__ import annotations

import csv
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from comun import config  # noqa: E402

COLUMNAS_UMBRALES = ["Rate", "Tot size", "IAT"]
PERCENTILES = [50, 75, 90, 95, 99]


def percentil(ordenados: list[float], p: float) -> float:
    """Percentil por el metodo del rango mas cercano."""
    n = len(ordenados)
    if n == 0:
        return 0.0
    indice = math.ceil((p / 100.0) * n) - 1
    indice = max(0, min(n - 1, indice))
    return float(ordenados[indice])


def main() -> None:
    ruta = Path(sys.argv[1]) if len(sys.argv) > 1 else config.CSV_MUESTRA
    valores: dict[str, list[float]] = {c: [] for c in COLUMNAS_UMBRALES}
    nulos: dict[str, int] = {c: 0 for c in COLUMNAS_UMBRALES}
    filas = 0

    with open(ruta, encoding="utf-8", newline="") as archivo:
        lector = csv.DictReader(archivo)
        for registro in lector:
            filas += 1
            for columna in COLUMNAS_UMBRALES:
                bruto = registro[columna]
                if bruto is None or bruto == "":
                    nulos[columna] += 1
                    continue
                valores[columna].append(float(bruto))

    umbrales: dict[str, dict[str, float]] = {}
    for columna in COLUMNAS_UMBRALES:
        # Se aplica el mismo redondeo de ingesta que los cuatro motores para que
        # el umbral escrito en el CSV de la operacion 05 sea exactamente el valor
        # que se compara en el filtro.
        datos = sorted(round(v, config.REDONDEO_INGESTA) for v in valores[columna])
        total = sum(datos)
        stats = {
            "conteo": len(datos),
            "nulos": nulos[columna],
            "min": datos[0] if datos else 0.0,
            "max": datos[-1] if datos else 0.0,
            "media": (total / len(datos)) if datos else 0.0,
        }
        for p in PERCENTILES:
            stats[f"p{p}"] = round(percentil(datos, p), config.REDONDEO_INGESTA)
        umbrales[columna] = stats

    contenido = {
        "metodo": "percentil por rango mas cercano",
        "redondeo_ingesta": config.REDONDEO_INGESTA,
        "filas_leidas": filas,
        "columnas": COLUMNAS_UMBRALES,
        "percentiles": PERCENTILES,
        "umbrales": umbrales,
    }
    config.RUTA_UMBRALES.parent.mkdir(parents=True, exist_ok=True)
    with open(config.RUTA_UMBRALES, "w", encoding="utf-8", newline="\n") as archivo:
        json.dump(contenido, archivo, ensure_ascii=False, indent=2)
        archivo.write("\n")

    print(f"Umbrales calculados sobre {filas:,} filas -> {config.RUTA_UMBRALES}")
    for columna in COLUMNAS_UMBRALES:
        print(f"  {columna}: " + ", ".join(f"p{p}={umbrales[columna][f'p{p}']:,.4f}" for p in PERCENTILES))


if __name__ == "__main__":
    main()
