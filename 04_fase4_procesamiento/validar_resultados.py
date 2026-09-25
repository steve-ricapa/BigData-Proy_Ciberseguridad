"""Valida que los cuatro motores de la Fase 4 producen los mismos resultados.

El script no ejecuta los motores: comprueba los archivos que cada notebook deja
en ``results/fase4/<motor>/``. Para los archivos de texto usa comparacion
exacta; para los CSV numericos acepta que dos celdas difieran solo si la
diferencia es menor que el ultimo decimal escrito, porque cada motor redondea
a ``config.DECIMALES`` al formatear.

Uso:

    python 04_fase4_procesamiento/validar_resultados.py
    python 04_fase4_procesamiento/validar_resultados.py polars dask modin spark
    python 04_fase4_procesamiento/validar_resultados.py --tabla

Devuelve 0 si todo coincide y 1 si hay algun problema.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from comun import config
from comun.catalogo_operaciones import CATALOGO

MOTORES = ["polars", "dask", "modin", "spark"]
REFERENCIA = "polars"

# El orden de las filas no esta garantizado por el motor: la muestra de la
# transformacion se valida como conjunto.
SIN_ORDEN = {"04_transformacion_muestra.csv"}

# Archivos que dependen de la maquina o del motor y no se comparan.
FUERA_DE_COMPARACION = {"00_tiempos.csv"}

# Dos celdas numericas se consideran iguales si difieren menos que esto.
TOLERANCIA_ABSOLUTA = 1e-6
TOLERANCIA_RELATIVA = 1e-9

# Valores que la muestra debe tener y que todos los motores deben reportar.
EXPECTATIVAS = {
    "filas": config.TOTAL_FILAS_ESPERADAS,
    "columnas": config.TOTAL_COLUMNAS_ESPERADAS,
    "etiquetas_distintas": config.TOTAL_ETIQUETAS_ESPERADAS,
}

# Claves que describen el motor y no el resultado: pueden ser distintas.
CLAVES_PROPIAS = {"motor", "version", "backend", "particiones", "bloque", "hilos"}


def archivos_esperados() -> list[str]:
    """Los CSV que el catalogo de operaciones exige, en orden de operacion."""
    return [a for op in CATALOGO for a in op.archivos if a.endswith(".csv")]


def json_esperados() -> list[str]:
    return [a for op in CATALOGO for a in op.archivos if a.endswith(".json")]


def leer_csv(ruta: Path) -> list[list[str]]:
    with open(ruta, encoding="utf-8", newline="") as archivo:
        return list(csv.reader(archivo))


def es_numero(texto: str) -> bool:
    try:
        float(texto)
    except (TypeError, ValueError):
        return False
    return True


def equivalentes(a: str, b: str) -> bool:
    if a == b:
        return True
    if not (es_numero(a) and es_numero(b)):
        return False
    x, y = float(a), float(b)
    if x == y:
        return True
    limite = max(TOLERANCIA_ABSOLUTA, TOLERANCIA_RELATIVA * max(abs(x), abs(y)))
    return abs(x - y) <= limite + 1e-12


def comparar_csv(referencia: Path, propia: Path) -> tuple[str, int]:
    """Devuelve (estado, celdas que no toleran la diferencia)."""
    a, b = leer_csv(referencia), leer_csv(propia)
    if not a or not b:
        return "vacio", 0
    if a[0] != b[0]:
        return f"cabecera distinta: {b[0]}", 0
    filas_a, filas_b = a[1:], b[1:]
    if referencia.name in SIN_ORDEN:
        # Las primeras cinco columnas son los datos originales de la muestra.
        # No ordenar por las columnas derivadas: son precisamente las que se
        # comparan y sus diferencias de redondeo cambiarian la posicion.
        filas_a = sorted(filas_a, key=lambda fila: tuple(fila[:5]))
        filas_b = sorted(filas_b, key=lambda fila: tuple(fila[:5]))
    if len(filas_a) != len(filas_b):
        return f"{len(filas_b)} filas en vez de {len(filas_a)}", 0

    distintas = [
        (i, x, y)
        for i, (x, y) in enumerate(zip(filas_a, filas_b))
        if x != y
    ]
    if not distintas:
        return "identico", 0
    toleradas = [d for d in distintas if not all(
        equivalentes(vx, vy) for vx, vy in zip(d[1], d[2])
    )]
    if not toleradas:
        return "identico con tolerancia", 0
    i, x, y = toleradas[0]
    detalle = ", ".join(f"{c}: {u} vs {v}" for c, u, v in zip(a[0], x, y) if u != v)
    return f"{len(toleradas)} celdas distintas (fila {i + 1}: {detalle})", len(toleradas)


def comparar_json(referencia: Path, propia: Path) -> str:
    """Compara las claves compartidas y descarta las propias de cada motor.

    ``motor``, ``version``, ``backend``, ``particiones``, ``bloque`` y ``hilos``
    describen la implementacion, no el resultado: no tienen por que coincidir.
    """
    a = json.load(open(referencia, encoding="utf-8"))
    b = json.load(open(propia, encoding="utf-8"))
    for clave in sorted(set(a) & set(b)):
        if clave in CLAVES_PROPIAS:
            continue
        if clave in EXPECTATIVAS and a[clave] != EXPECTATIVAS[clave]:
            return f"{clave}={a[clave]} (se esperaba {EXPECTATIVAS[clave]})"
        if not equivalentes(str(a[clave]), str(b[clave])):
            return f"{clave}: {a[clave]} vs {b[clave]}"
    return "consistente"


def validar(motores: list[str], detalle: bool) -> int:
    base = config.RUTA_RESULTADOS
    problemas: list[str] = []
    print(f"Referencia: {REFERENCIA}")
    print(f"Motores:    {', '.join(motores)}")
    print(f"Resultados: {base}\n")

    print("1. Archivos obligatorios del catalogo de operaciones")
    for motor in motores:
        faltan_csv = [a for a in archivos_esperados() if not (base / motor / a).exists()]
        faltan_json = [a for a in json_esperados() if not (base / motor / a).exists()]
        if faltan_csv or faltan_json:
            problemas.append(f"{motor}: faltan {faltan_csv + faltan_json}")
            print(f"   {motor:8s} FALTAN {len(faltan_csv) + len(faltan_json)} archivos")
        else:
            print(f"   {motor:8s} {len(archivos_esperados())} CSV y "
                  f"{len(json_esperados())} JSON presentes")

    print("\n2. Consistencia de los resumenes JSON con la muestra")
    for motor in motores:
        notas = []
        for nombre in json_esperados():
            ruta = base / motor / nombre
            if not ruta.exists():
                continue
            if motor == REFERENCIA:
                estado = comparar_json(ruta, ruta)
            else:
                estado = comparar_json(base / REFERENCIA / nombre, ruta)
            if estado != "consistente":
                notas.append(f"{nombre}: {estado}")
                problemas.append(f"{motor}/{nombre}: {estado}")
        print(f"   {motor:8s} " + ("consistente" if not notas else "; ".join(notas)))

    print("\n3. Resultados identicos entre motores")
    encabezado = f"   {'archivo':38s} " + " ".join(f"{m:>26s}" for m in motores[1:])
    print(encabezado)
    for nombre in archivos_esperados():
        if nombre in FUERA_DE_COMPARACION:
            continue
        ruta_ref = base / REFERENCIA / nombre
        if not ruta_ref.exists():
            continue
        celdas = []
        for motor in motores[1:]:
            ruta = base / motor / nombre
            if not ruta.exists():
                estado, malas = "FALTA", 1
            else:
                estado, malas = comparar_csv(ruta_ref, ruta)
            if estado not in ("identico", "identico con tolerancia"):
                problemas.append(f"{motor}/{nombre}: {estado}")
            if detalle and estado not in ("identico", "identico con tolerancia"):
                print(f"\n   detalle de {motor}/{nombre}")
                a, b = leer_csv(ruta_ref), leer_csv(ruta)
                for i, (x, y) in enumerate(zip(a[1:], b[1:])):
                    if x != y and not all(equivalentes(u, v) for u, v in zip(x, y)):
                        print(f"     fila {i + 1}: {x} vs {y}")
            celdas.append(f"{estado:>26s}")
        print(f"   {nombre:38s} " + " ".join(celdas))

    print("\n4. Tiempos de ejecucion registrados")
    for motor in motores:
        ruta = base / motor / "00_tiempos.csv"
        if not ruta.exists():
            problemas.append(f"{motor}: sin 00_tiempos.csv")
            print(f"   {motor:8s} sin registro de tiempos")
            continue
        filas = leer_csv(ruta)[1:]
        ids = [f[1] for f in filas]
        esperados = ["00"] + [op.id for op in CATALOGO]
        faltan = [i for i in esperados if i not in ids]
        if faltan:
            problemas.append(f"{motor}/00_tiempos.csv: sin los pasos {faltan}")
        total = sum(float(f[3]) for f in filas)
        print(f"   {motor:8s} {len(filas)} pasos, {total:8.3f} s en total"
              + (f"   FALTAN {faltan}" if faltan else ""))

    print()
    if problemas:
        print(f"RESULTADO: {len(problemas)} problema(s)")
        for problema in problemas:
            print(f"  - {problema}")
        return 1
    print(f"RESULTADO: los {len(motores)} motores coinciden en "
          f"{len(archivos_esperados())} archivos de resultados.")
    return 0


def tabla_markdown(motores: list[str]) -> str:
    """Tabla de tiempos para pegar en el informe."""
    filas = ["Id", "Operación"] + motores
    SEPARADOR = ["---"] * len(filas)
    tiempos: dict[str, dict[str, float]] = {}
    operaciones: dict[str, str] = {}
    for motor in motores:
        ruta = config.RUTA_RESULTADOS / motor / "00_tiempos.csv"
        if not ruta.exists():
            continue
        for f in leer_csv(ruta)[1:]:
            tiempos.setdefault(f[1], {})[motor] = float(f[3])
            operaciones[f[1]] = f[2]
    cuerpo = []
    for op_id in ["00"] + [op.id for op in CATALOGO]:
        if op_id not in tiempos:
            continue
        valores = tiempos[op_id]
        cuerpo.append(
            [op_id, operaciones[op_id]]
            + [f"{valores[motor]:.3f}" if motor in valores else "-" for motor in motores]
        )
    total = ["", "Total"]
    total[2:] = [
        f"{sum(tiempos[i].get(motor, 0.0) for i in tiempos):.3f}" for motor in motores
    ]
    cuerpo.append(total)
    lineas = ["| " + " | ".join(filas) + " |", "| " + " | ".join(SEPARADOR) + " |"]
    lineas += ["| " + " | ".join(f) + " |" for f in cuerpo]
    return "\n".join(lineas)


if __name__ == "__main__":
    analizador = argparse.ArgumentParser(description=__doc__)
    analizador.add_argument("motores", nargs="*", default=None)
    analizador.add_argument("--detalle", action="store_true",
                            help="muestra la fila exacta de cada diferencia")
    analizador.add_argument("--tabla", action="store_true",
                            help="imprime la tabla de tiempos en Markdown")
    opciones = analizador.parse_args()
    motores = opciones.motores or MOTORES
    if opciones.tabla:
        print(tabla_markdown(motores))
        raise SystemExit(0)
    raise SystemExit(validar(motores, opciones.detalle))
