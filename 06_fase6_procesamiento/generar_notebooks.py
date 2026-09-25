"""Sincroniza notebooks scaffolded con los bloques Python de cada motor.

El archivo .py es la fuente editable; el notebook incorpora ese código completo
en 10 celdas separadas, sin depender del .py al ejecutar.
"""

import json
import uuid
from pathlib import Path

FASE = Path(__file__).resolve().parent
PRE = '''import sys
from pathlib import Path
actual = Path.cwd().resolve()
for candidato in [actual, *actual.parents]:
    fase = candidato / "06_fase6_procesamiento"
    if (fase / "comun.py").is_file():
        sys.path.insert(0, str(fase))
        break
else:
    raise RuntimeError("Ejecutar el notebook dentro del repositorio")
'''
INSTRUCCIONES = {
    "Q1": "Valida las 600.000 filas originales (sin limpiar) y cuenta nulos, infinitos y duplicados exactos.",
    "Q2": "Deduplicación lógica sobre las 40 columnas; la muestra original permanece intacta.",
    "Q3": "Transforma Label en una de 8 familias, comprueba la cobertura de las 34 etiquetas.",
    "Q4": "Filtra Label != Benign sin reemplazar el dataset original.",
    "Q5": "Crea Traffic_Type y resume benigno vs malicioso.",
    "Q6": "Agrupa por familia y ordena por cantidad descendente.",
    "Q7": "Top 10 de clases maliciosas; porcentaje sobre los 600.000 registros totales.",
    "Q8": "Sustituye los 13 Rate infinitos por nulos SOLO en Rate; IAT usa las 600.000 filas.",
    "Q9": "Mapea el número IP Protocol Type; no usa las columnas agregadas TCP/UDP/ICMP.",
    "Q10": "Compara los dos tipos de tráfico. Rate excluye 13 infinitos; las demás métricas no.",
}


def celda(tipo, texto):
    c = {"cell_type": tipo, "id": uuid.uuid5(uuid.NAMESPACE_URL, tipo + texto).hex[:8],
         "metadata": {}, "source": texto.splitlines(keepends=True)}
    if tipo == "code":
        c.update(execution_count=None, outputs=[])
    return c


for motor in ("polars", "dask", "modin", "spark"):
    ruta = FASE / motor / f"{motor}_consultas.ipynb"
    cuaderno = json.loads(ruta.read_text(encoding="utf-8"))
    bloques = (FASE / motor / "consultas.py").read_text(encoding="utf-8").split("# %% ")
    partes = {}
    for bloque in bloques[1:]:
        titulo, codigo = bloque.split("\n", 1)
        partes[titulo] = codigo
    assert list(partes) == ["PREPARACION", *INSTRUCCIONES, "CIERRE"]
    c = [celda("markdown", f"# Fase 6 — {motor.title()} · CIC-IoT-2023\n\n"
             "600.000 filas, 40 columnas · 10 consultas idénticas por motor. "
             "Los resultados se guardan en `06_fase6_procesamiento/results/<motor>/`. "
             "No se modifica la muestra original. Ejecutar **Run All** de arriba abajo."),
         celda("markdown", "## Configuración y carga\n\nLa carga queda fuera de los tiempos Q1–Q10. "
               "Las operaciones perezosas se materializan dentro de cada consulta."),
         celda("code", PRE + partes["PREPARACION"])]
    for id_, texto in INSTRUCCIONES.items():
        c.extend([celda("markdown", f"## {id_} — {texto}"),
                  celda("code", partes[id_])])
    c.extend([celda("markdown", "## Tiempos por consulta\n\nTiempos de pared (segundos). "
                    "Comparar solo con atención a carga, caché, hilos y materialización."),
              celda("code", partes["CIERRE"])])
    previas = {"".join(v["source"]): v for v in cuaderno["cells"]
                if v["cell_type"] == "code" and v.get("execution_count") is not None}
    for nueva in c:
        anterior = previas.get("".join(nueva["source"]))
        if anterior and nueva["cell_type"] == "code":
            nueva["execution_count"] = anterior["execution_count"]
            nueva["outputs"] = anterior["outputs"]
    cuaderno["cells"] = c
    cuaderno["metadata"]["kernelspec"] = {
        "display_name": "Python 3", "language": "python", "name": "python3"}
    ruta.write_text(json.dumps(cuaderno, ensure_ascii=False, indent=1) + "\n",
                    encoding="utf-8")
    print(ruta, "celdas=", len(c), "consultas=", len(INSTRUCCIONES))
