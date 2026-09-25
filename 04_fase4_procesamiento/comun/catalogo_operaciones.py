"""Catalogo de las 10 operaciones de la Fase 4.

Cada operacion declara a que requisito del enunciado responde y que archivos
genera. El validador y los reportes se construyen a partir de este catalogo.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Operacion:
    id: str
    nombre: str
    requisito: str
    descripcion: str
    archivos: tuple[str, ...] = field(default_factory=tuple)


CATALOGO: tuple[Operacion, ...] = (
    Operacion(
        id="01",
        nombre="Carga y validacion del dataset",
        requisito="Validacion",
        descripcion=(
            "Verifica filas, columnas, tipos normalizados, valores nulos, "
            "minimo, maximo y numero de valores distintos por columna."
        ),
        archivos=("01_validacion.csv", "01_validacion_resumen.json"),
    ),
    Operacion(
        id="02",
        nombre="Limpieza de valores nulos",
        requisito="Limpieza de datos",
        descripcion=(
            "Elimina los registros que contienen valores nulos y cuantifica "
            "el impacto por columna."
        ),
        archivos=("02_limpieza.csv", "02_limpieza_resumen.json"),
    ),
    Operacion(
        id="03",
        nombre="Tratamiento de duplicados",
        requisito="Eliminacion de duplicados",
        descripcion=(
            "Mide y elimina duplicados exactos y duplicados por clave "
            "(Label, Protocol Type, Tot size, IAT, Rate, Number)."
        ),
        archivos=("03_duplicados.csv", "03_duplicados_ejemplos.csv"),
    ),
    Operacion(
        id="04",
        nombre="Transformacion de variables",
        requisito="Transformacion de variables",
        descripcion=(
            "Crea size_kb, rate_mbps, coef_variacion, total_flags, "
            "protocolo_principal y rango_iat."
        ),
        archivos=("04_transformacion_variables.csv", "04_transformacion_muestra.csv"),
    ),
    Operacion(
        id="05",
        nombre="Filtrado de trafico",
        requisito="Filtrado",
        descripcion=(
            "Aplica cuatro filtros basados en percentiles calculados de forma "
            "comun y reporta la cantidad de registros que cumple cada uno."
        ),
        archivos=("05_filtrado.csv", "05_filtrado_por_label.csv"),
    ),
    Operacion(
        id="06",
        nombre="Agregaciones globales",
        requisito="Agregaciones",
        descripcion=(
            "Calcula conteo, suma, media, mediana, minimo, maximo y "
            "desviacion estandar de las columnas indicadoras."
        ),
        archivos=("06_agregaciones.csv",),
    ),
    Operacion(
        id="07",
        nombre="Agrupaciones por clase y protocolo",
        requisito="Agrupaciones",
        descripcion=(
            "Agrupa por Label y Protocol Type con registros, porcentaje, "
            "media de tamano, media de tasa y volumen total."
        ),
        archivos=("07_agrupaciones.csv",),
    ),
    Operacion(
        id="08",
        nombre="Ordenamiento y Top-N",
        requisito="Ordenamiento",
        descripcion="Ordena las clases por volumen de trafico y muestra el Top 10.",
        archivos=("08_ordenamiento_top10.csv",),
    ),
    Operacion(
        id="09",
        nombre="Metricas de ciberseguridad",
        requisito="Calculo de metricas",
        descripcion=(
            "Calcula indicadores de poblacion, volumen, comportamiento de "
            "flags, concentracion de ataques y calidad de los datos."
        ),
        archivos=("09_metricas_ciberseguridad.csv",),
    ),
    Operacion(
        id="10",
        nombre="Resumen consolidado por clase",
        requisito="CRUD y tablas de resultado",
        descripcion=(
            "Construye la tabla final por Label uniendo agregaciones, "
            "metricas y variables transformadas."
        ),
        archivos=("10_resumen_consolidado.csv",),
    ),
)

TOTAL_OPERACIONES = len(CATALOGO)


def resumen_catalogo() -> list[dict]:
    return [
        {
            "id": op.id,
            "nombre": op.nombre,
            "requisito": op.requisito,
            "descripcion": op.descripcion,
            "archivos": list(op.archivos),
        }
        for op in CATALOGO
    ]
