"""Compara las 40 consultas materializadas; falla ante diferencias reales."""

import csv
import json
import math
from pathlib import Path

from comun import FASE, NOMBRES, RESULTADOS

MOTORES = ("polars", "dask", "modin", "spark")
CLAVES = {
    "Q3": ("Attack_Family",), "Q5": ("Traffic_Type",),
    "Q6": ("Attack_Family",), "Q7": ("Label",),
    "Q8": ("Attack_Family",), "Q9": ("Attack_Family", "Protocol Type"),
    "Q10": ("Traffic_Type",),
}


def cargar(archivo):
    with archivo.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def comparar(referencia, actual, id_, motor):
    assert len(actual) == len(referencia), (id_, motor, "número de filas")
    claves = CLAVES.get(id_)
    if claves:
        referencia = sorted(referencia, key=lambda r: tuple(r[k] for k in claves))
        actual = sorted(actual, key=lambda r: tuple(r[k] for k in claves))
    for i, (r, a) in enumerate(zip(referencia, actual)):
        assert r.keys() == a.keys(), (id_, motor, "columnas")
        for col in r:
            if r[col] == a[col]:
                continue
            try:
                ok = math.isclose(float(r[col]), float(a[col]), rel_tol=1e-9, abs_tol=1e-6)
            except ValueError:
                ok = False
            assert ok, (id_, motor, i, col, r[col], a[col])


def main():
    for motor in MOTORES:
        for id_ in list(NOMBRES)[:10]:
            ruta = RESULTADOS / motor / NOMBRES[id_]
            assert ruta.is_file(), ruta
            actual = cargar(ruta)
            referencia = cargar(RESULTADOS / "polars" / NOMBRES[id_])
            comparar(referencia, actual, id_, motor)
        tiempos = cargar(RESULTADOS / motor / "tiempos.csv")
        assert len(tiempos) == 10
        assert [fila["Consulta"] for fila in tiempos] == [f"Q{i}" for i in range(1, 11)]
        for fila in tiempos:
            assert float(fila["Tiempo"]) >= 0
        ruta = FASE / motor / f"{motor}_consultas.ipynb"
        notebook = json.loads(ruta.read_text(encoding="utf-8"))
        celdas = [c for c in notebook["cells"] if c["cell_type"] == "code"]
        assert len(celdas) == 12, (motor, len(celdas))
        assert [c["execution_count"] for c in celdas] == list(range(1, 13))
        assert all(c["outputs"] for c in celdas), motor
        assert not any(o["output_type"] == "error" for c in celdas for o in c["outputs"]), motor
        print(f"{motor}: 10 CSV + tiempos + 12 celdas ejecutadas; coincide con Polars")
    print("OK: 4 notebooks; 40 consultas equivalentes; 44 CSV; 40 tiempos")


if __name__ == "__main__":
    main()
