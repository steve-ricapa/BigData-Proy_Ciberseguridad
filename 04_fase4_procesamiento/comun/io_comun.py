"""Escritura normalizada de resultados.

Todos los motores pasan por aqui para que los CSV sean identicos byte a byte:
mismos nombres de columna, mismo orden, mismos decimales y mismos saltos de
linea. Asi la comparacion entre motores no depende del motor.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Iterable, Sequence

from . import config


def _a_escalar(valor: Any) -> Any:
    """Convierte escalares de numpy o Spark a tipos nativos de Python."""
    if valor is None:
        return None
    if hasattr(valor, "item") and not isinstance(valor, (str, bytes)):
        try:
            return valor.item()
        except (ValueError, AttributeError):
            return valor
    return valor


def formatear(valor: Any) -> str:
    """Representa un valor de forma determinista para el CSV."""
    valor = _a_escalar(valor)

    if valor is None:
        return ""
    if isinstance(valor, bool):
        return "1" if valor else "0"
    if isinstance(valor, int):
        return str(valor)
    if isinstance(valor, float):
        if math.isnan(valor) or math.isinf(valor):
            return ""
        texto = f"{valor:.{config.DECIMALES}f}".rstrip("0")
        return texto + "0" if texto.endswith(".") else texto
    return str(valor)


def escribir_csv(
    destino: Path, columnas: Sequence[str], registros: Iterable[dict]
) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", encoding="utf-8", newline="") as archivo:
        escritor = csv.writer(archivo, lineterminator="\n")
        escritor.writerow(list(columnas))
        for registro in registros:
            escritor.writerow([formatear(registro.get(c)) for c in columnas])
    return destino


def escribir_json(destino: Path, contenido: Any) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)

    def limpio(objeto: Any) -> Any:
        if isinstance(objeto, dict):
            return {k: limpio(v) for k, v in objeto.items()}
        if isinstance(objeto, (list, tuple)):
            return [limpio(v) for v in objeto]
        valor = _a_escalar(objeto)
        if isinstance(valor, float):
            if math.isnan(valor) or math.isinf(valor):
                return None
            return round(valor, config.DECIMALES)
        return valor

    with open(destino, "w", encoding="utf-8") as archivo:
        json.dump(limpio(contenido), archivo, ensure_ascii=False, indent=2)
        archivo.write("\n")
    return destino


def escribir_texto(destino: Path, texto: str) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", encoding="utf-8", newline="\n") as archivo:
        archivo.write(texto)
    return destino


def leer_csv(destino: Path) -> tuple[list[str], list[list[str]]]:
    with open(destino, encoding="utf-8", newline="") as archivo:
        lector = csv.reader(archivo)
        filas = list(lector)
    if not filas:
        return [], []
    return filas[0], filas[1:]


def pct(parte: float, total: float) -> float:
    """Porcentaje protegido contra division por cero."""
    if not total:
        return 0.0
    return (parte / total) * 100.0
