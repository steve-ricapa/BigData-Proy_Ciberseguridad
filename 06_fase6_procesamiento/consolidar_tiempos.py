"""Reconstruye el comparativo de tiempos desde los cuatro CSV de ejecución."""

import csv
from pathlib import Path

from comun import RESULTADOS


def main():
    motores = ("polars", "dask", "modin", "spark")
    tabla = {}
    for motor in motores:
        with (RESULTADOS / motor / "tiempos.csv").open(encoding="utf-8", newline="") as f:
            datos = list(csv.DictReader(f))
        assert [r["Consulta"] for r in datos] == [f"Q{i}" for i in range(1, 11)]
        tabla[motor] = {r["Consulta"]: float(r["Tiempo"]) for r in datos}
    ruta = RESULTADOS / "comparativo_tiempos.csv"
    with ruta.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Consulta", *motores], lineterminator="\n")
        w.writeheader()
        for i in range(1, 11):
            q = f"Q{i}"
            w.writerow({"Consulta": q, **{m: tabla[m][q] for m in motores}})
        w.writerow({"Consulta": "TOTAL_Q1_Q10",
                    **{m: round(sum(tabla[m].values()), 3) for m in motores}})
    print(ruta.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
