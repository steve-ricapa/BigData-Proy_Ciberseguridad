# Fase 4 — Procesamiento distribuido: resultados

## Alcance y entregables

Se ejecutaron **10 operaciones × 4 motores = 40 consultas** sobre la misma
muestra local de CIC-IoT-2023 (600 000 filas, 40 columnas). El código fuente de
cada motor está en el notebook correspondiente de `04_fase4_procesamiento/`.
Los resultados están en `results/fase4/<motor>/` (13 CSV y 4 archivos de
control por motor: dos JSON, tiempos CSV y resumen JSON). Esta fase genera
tablas; la interpretación y las preguntas de investigación pertenecen a la
Fase 6. Hadoop/Dataproc queda para la Fase 5.

| Consulta | Requisito | Producto |
|---|---|---|
| 01 | Validación | Esquema, valores inválidos, distintos y extremos por columna |
| 02 | Limpieza | Registros limpios y efecto de nulos/infinito |
| 03 | Duplicados | Duplicados exactos y por clave |
| 04 | Transformación de variables | Seis variables derivadas y muestra |
| 05 | Filtrado | Cuatro filtros por percentiles y desglose por clase |
| 06 | Agregaciones | Conteo, suma, media, mediana, mínimos, máximos y desviación |
| 07 | Agrupaciones | Por clase (`Label`) y tipo de protocolo |
| 08 | Ordenamiento | Diez clases con mayor volumen de bytes |
| 09 | Métricas | Indicadores de población, volumen, flags, concentración y calidad |
| 10 | Tabla de resultados | Resumen consolidado por clase |

### Equivalencia con CRUD

CRUD aquí se muestra **como operaciones equivalentes sobre DataFrames y archivos**,
no como sentencias SQL ni como servicio de base de datos:

| CRUD | Equivalencia realizada |
|---|---|
| Create | Se crean columnas derivadas (04) y se escriben CSV de resultados (01–10). |
| Read | Se lee la muestra local y se consultan filtros, agregados y agrupaciones (01, 05–09). |
| Update | Se actualizan los valores representados mediante limpieza y transformación (02, 04), conservando el CSV original. |
| Delete | Se excluyen filas inválidas y se identifican/quitan duplicados en la operación 03, sin borrar el archivo fuente. |

La tabla 10 une métricas y agregados por etiqueta; equivale a una **vista
analítica materializada en CSV**, no a una vista de una base de datos.

## Resultado del procesamiento

- La limpieza descarta 13 filas con valores no válidos: quedan **599 987**.
  `Rate` tiene 13 infinitos; `Std` y `Variance` tienen 9 nulos cada una,
  solapados en las mismas filas eliminadas.
- Duplicados completos: **204 491** (34,082572 % de las filas limpias);
  únicos completos: **395 496**. Duplicados por la clave analítica de la
  operación 03: **254 085** (42,348418 %); únicos por clave: **345 902**.
  Por eso `pct_filas_unicas` en la operación 09 es **65,917428 %** de las
  filas ya limpias, no el porcentaje de registros que sobrevivieron a la limpieza.
- La muestra limpia conserva **34 clases** (33 ataques y tráfico benigno);
  **14 085** registros son `Benign` (2,347551 % de las filas limpias).
- Según `08_ordenamiento_top10.csv`, `Benign` lidera **volumen total de bytes**
  (8 575 966,255556). Según `10_resumen_consolidado.csv`,
  `DDoS-ICMP_Flood` lidera **número de registros** (92 356).
- Los percentiles de filtrado se calculan una vez en `comun/umbrales.json`
  sobre las cifras redondeadas a seis decimales; los motores comparten umbrales.

## Comprobación entre motores

`python -X utf8 04_fase4_procesamiento/validar_resultados.py` pasa sin errores:
**13 de 13 CSV comparables para Polars, Dask, Modin y Spark**. La tolerancia
para diferencias de representación numérica es máx(1e-6 absoluto,
1e-9 relativo); no oculta diferencias en etiquetas, conteos o cabeceras.
Los once pasos cronometrados por motor se recogen en `fase4_tiempos.csv`.

**Precaución con los tiempos:** son una corrida local por motor, no un
benchmark estadístico, y no deben interpretarse como rendimiento en clúster.
Polars y Modin cargan el archivo de inmediato; Dask y Spark construyen planes
perezosos, por lo que la lectura efectiva puede ocurrir durante otras
operaciones. Los costos de inicio del runtime y de materialización afectan
la comparación. No se usó Dataproc en esta fase.

## Alcance de los datos

No hay IP de origen/destino ni marcas de tiempo en la muestra de 40 columnas.
No es posible responder preguntas de atribución de direcciones, secuencias
temporales o causas de ataques sin otra fuente. Los CSV de esta fase son
agregados y muestras de resultados; no son una tabla transformada con todas las
filas, por lo que para preguntas nuevas de granularidad por registro hay que
volver a leer la muestra original y aplicar la transformación necesaria.
