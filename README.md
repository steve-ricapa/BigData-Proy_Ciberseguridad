# BigData-Proy_Ciberseguridad

Proyecto Grupal I de Big Data: almacenamiento y procesamiento de una muestra del
dataset **CIC-IoT-2023** con Google Cloud Storage, Polars, Dask, Modin, Apache
Spark y Hadoop MapReduce en Dataproc.

## Integrantes

- Santiago Aldebaran Cama Ardiles
- Cesar Stefano Flores Uriarte
- Steve Bryan Ricapa Montoya

**Semestre:** 2026-2.

## Qué se implementó

| Componente | Estado y evidencia principal |
|---|---|
| Datos | Muestra CSV de **600.000 filas y 40 columnas**, generada desde 309 CSV originales mediante muestreo reproducible con semilla 42. Véase [`01_fase1_datos/calidad/CICIoT2023_sample_600k_report.json`](01_fase1_datos/calidad/CICIoT2023_sample_600k_report.json). |
| Data Lake | Bucket `gs://ciciot2023-bigdata-utec-proyecto1-gm/` en el proyecto `bigdata-proyecto1-ciciot2023`. Los logs de Fase 5 registran la lectura de la muestra desde `processed/` y la exportación de resultados a `results/`. |
| Arquitectura | Diseño, diagrama Mermaid y decisiones técnicas en [`03_fase3_arquitectura/documentacion/arquitectura_big_data.md`](03_fase3_arquitectura/documentacion/arquitectura_big_data.md). |
| Procesamiento | **10 operaciones × 4 motores** en Fase 4 y, por separado, **10 consultas equivalentes × 4 motores** en Fase 6. Notebooks ejecutados, resultados CSV y validadores. |
| Hadoop | `secondarysort`, `terasort` y `pi` del JAR de ejemplos de Hadoop, ejecutados en el clúster temporal `ciciot-fase5`. Entradas, salidas HDFS, metadatos y logs conservados; clúster eliminado al terminar. |

Las consultas de Polars, Dask, Modin y Spark se ejecutaron **localmente**, no en
Dataproc. Dataproc se usó para Hadoop/HDFS en la Fase 5. El diagrama de Fase 3
incluye componentes de diseño que no deben confundirse con ejecuciones reales.

## Dataset y resultados destacados

- Muestra: [`01_fase1_datos/muestra/CICIoT2023_sample_600k.csv`](01_fase1_datos/muestra/CICIoT2023_sample_600k.csv), **124.498.206 bytes (118,731 MiB)**, versionada con Git LFS. Los originales de `01_fase1_datos/raw/` no se versionan en Git.
- Muestreo: dos pasadas de lectura *streaming*, selección aleatoria sin reemplazo, `random_state=42` y representación de las 34 clases. Código: [`01_fase1_datos/scripts/prepare_cic_iot_2023.py`](01_fase1_datos/scripts/prepare_cic_iot_2023.py).
- Calidad de la muestra: **18 celdas nulas**, **13 valores infinitos en `Rate`**, **131.030 registros duplicados exactos** y **468.970 registros distintos**.
- Tráfico: **14.087** registros `Benign` (2,348 %) y **585.913** maliciosos (97,652 %). La familia DDoS suma **435.902** (72,650 %); la etiqueta más frecuente es `DDoS-ICMP_Flood` con **92.356** registros.

Estas cifras provienen de los CSV de
[`06_fase6_procesamiento/results/polars/`](06_fase6_procesamiento/results/polars/)
y se compararon con los resultados de los otros tres motores. Son estadísticas
**descriptivas de la muestra**, no métricas de un detector de ataques.

## Organización del repositorio

```text
01_fase1_datos/                  Muestra, script de muestreo, calidad y documentación CSV
02_fase2_gcp/                    Carpetas reservadas; sin capturas ni scripts propios versionados
03_fase3_arquitectura/           Documento con diagrama Mermaid y justificación
04_fase4_procesamiento/          Cuatro notebooks, contrato de operaciones y validador
05_fase5_hadoop/                 Entradas Hadoop, orquestación HDFS, logs y manifiesto
06_fase6_procesamiento/          Cuatro notebooks Q1–Q10, código, resultados y tiempos
06_fase6_analisis/               Estructura reservada para análisis posterior
07_fase7_entregables/            Estructura reservada para entregables
results/fase4/                   CSV/JSON de las operaciones de Fase 4
results/fase5/mapreduce/         Salidas Hadoop copiadas desde HDFS
reports/                         Resúmenes de Fases 4 y 5
```

### Fases y documentación

| Fase | Qué consultar |
|---|---|
| 1 — Dataset | [`Reporte de muestreo y calidad`](01_fase1_datos/calidad/CICIoT2023_sample_600k_report.json) y [`script`](01_fase1_datos/scripts/prepare_cic_iot_2023.py). |
| 2 — GCP | [Diseño de Data Lake](03_fase3_arquitectura/documentacion/arquitectura_big_data.md) y evidencia operativa de lectura/escritura en [`05_fase5_hadoop/logs/`](05_fase5_hadoop/logs/). `02_fase2_gcp/` no contiene capturas. |
| 3 — Arquitectura | [`arquitectura_big_data.md`](03_fase3_arquitectura/documentacion/arquitectura_big_data.md); el diagrama está en Mermaid dentro del Markdown, no en una imagen versionada. |
| 4 — Operaciones con cuatro motores | [`README de Fase 4`](04_fase4_procesamiento/README.md), [`resumen`](reports/fase4_resumen.md) y [`resultados`](results/fase4/). |
| 5 — Hadoop/Dataproc | [`README de Fase 5`](05_fase5_hadoop/README.md), [`resumen`](reports/fase5_resumen.md), [`manifiesto SHA-256`](05_fase5_hadoop/resultados/manifiesto.json) y [`salidas`](results/fase5/mapreduce/). |
| 6 — Consultas equivalentes | [`README de Fase 6`](06_fase6_procesamiento/README.md), [`comparativo de tiempos`](06_fase6_procesamiento/results/comparativo_tiempos.csv) y [`validación`](06_fase6_procesamiento/logs/validacion.txt). |

## Consultas de la Fase 6

Los notebooks
[`polars`](06_fase6_procesamiento/polars/polars_consultas.ipynb),
[`dask`](06_fase6_procesamiento/dask/dask_consultas.ipynb),
[`modin`](06_fase6_procesamiento/modin/modin_consultas.ipynb) y
[`spark`](06_fase6_procesamiento/spark/spark_consultas.ipynb) contienen las
**mismas diez consultas** sobre la muestra sin modificar:

| ID | Operación | Archivo de salida por motor |
|---|---|---|
| Q1 | Validación: filas, columnas, nulos, infinitos y duplicados | `q01_validacion.csv` |
| Q2 | Deduplicación exacta lógica | `q02_duplicados.csv` |
| Q3 | Derivar `Attack_Family` desde `Label` | `q03_familias.csv` |
| Q4 | Filtrar registros maliciosos | `q04_malicioso.csv` |
| Q5 | Benigno frente a malicioso | `q05_tipo_trafico.csv` |
| Q6 | Distribución y ordenamiento por familia | `q06_distribucion_familias.csv` |
| Q7 | Diez etiquetas de ataque más frecuentes | `q07_top10_ataques.csv` |
| Q8 | Estadísticas de `Rate` e `IAT` por familia | `q08_rate_iat_familia.csv` |
| Q9 | Distribución de `Protocol Type` dentro de cada familia | `q09_protocolos_familia.csv` |
| Q10 | Perfil estadístico benigno/malicioso | `q10_perfil_trafico.csv` |

Las salidas están en `06_fase6_procesamiento/results/<motor>/`. El registro
[`logs/validacion.txt`](06_fase6_procesamiento/logs/validacion.txt) confirma
**40 consultas equivalentes, 44 CSV y cuatro notebooks ejecutados**.

## Hadoop, HDFS y tiempos

El CSV de la muestra se copió de GCS a
`hdfs:///fase5/entrada/csv/CICIoT2023_sample_600k.csv`. Para `secondarysort`
y `terasort` se generaron entradas compatibles a partir de **599.987** filas
con `Rate` finita. `pi` estimó **3,1414** con 4 × 10.000 muestras y **no usa el
CSV como entrada**. Su resultado se escribió posteriormente en HDFS desde el
log del driver. Los tres jobs terminaron `DONE / FINISHED`; detalles y comandos
en [`05_fase5_hadoop/README.md`](05_fase5_hadoop/README.md). La eliminación
del clúster consta en `logs/cluster_delete.log` y
`logs/cluster_deleted_verified.txt`.

Suma de tiempos Q1–Q10: **Polars 1,229 s; Dask 65,197 s; Modin 503,460 s;
Spark 36,227 s**, según el
[`CSV comparativo`](06_fase6_procesamiento/results/comparativo_tiempos.csv).
Fue una sola ejecución en Windows 11 con motores y cachés distintos: **no es
un benchmark universal**.

## Reproducibilidad y límites

La instalación de dependencias y los pasos para ejecutar cada fase están en
sus READMEs. Para los notebooks se usó Python 3.12 y las versiones fijadas en
[`04_fase4_procesamiento/requirements.txt`](04_fase4_procesamiento/requirements.txt)
(Spark requiere Java 21). Para verificar resultados ya generados, sin crear
recursos cloud:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\04_fase4_procesamiento\validar_resultados.py
.\.venv\Scripts\python.exe -X utf8 .\05_fase5_hadoop\validar_resultados.py
.\.venv\Scripts\python.exe -X utf8 .\06_fase6_procesamiento\validar_resultados.py
```

**No regenerar la muestra ni desplegar Dataproc solo para consultar el
proyecto:** el script de muestreo requiere los CSV originales y escribe la
muestra y el reporte; los comandos de Dataproc crean recursos con posibles
costos. La comparación de motores se hizo sobre el CSV local. No hay IP,
puertos, timestamps ni duración de flujo en la muestra; `Time_To_Live` no es
una duración y `Protocol Type` no equivale a las columnas `TCP`, `UDP` o `ICMP`.

La configuración y las credenciales de Google Cloud **no se versionan**. No
subir archivos como `credentials.db`, `access_tokens.db` o
`application_default_credentials.json`.

## Repositorio

[BigData-Proy_Ciberseguridad en GitHub](https://github.com/steve-ricapa/BigData-Proy_Ciberseguridad)
— remote configurado: `git@github.com:steve-ricapa/BigData-Proy_Ciberseguridad.git`.
