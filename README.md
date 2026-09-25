# BigData-Proy_Ciberseguridad

Proyecto universitario para el análisis distribuido del dataset CIC-IoT-2023 en Google Cloud.

## Estado actual

- **Fases completadas:** Fase 1 — Dataset; Fase 2 — Configuración de GCP; Fase 3 — Diseño de arquitectura; Fase 4 — Procesamiento con cuatro motores; Fase 5 — Hadoop/MapReduce en Dataproc; Fase 6 — Procesamiento distribuido con diez consultas equivalentes por motor.
- **Siguiente fase:** Análisis de indicadores y preguntas finales de ciberseguridad.
- **Documento de arquitectura:** `03_fase3_arquitectura/documentacion/arquitectura_big_data.md`
- **Proyecto GCP:** `bigdata-proyecto1-ciciot2023`
- **Bucket:** `gs://ciciot2023-bigdata-utec-proyecto1-gm/`
- **Región del bucket:** `US-CENTRAL1`
- **Muestra local:** 600,000 registros y 40 columnas.
- **Dataset original local:** 309 CSV, 34 clases y 46,776,700 registros.
- **Dataproc:** el clúster temporal de Fase 5 se eliminó tras guardar las evidencias; resultados y configuración permanecen en `results/fase5/` y `05_fase5_hadoop/`.
- **GKE:** no habilitado.

## Estructura local

```text
01_fase1_datos/
├── raw/             # 309 CSV originales del CIC-IoT-2023
├── muestra/         # CICIoT2023_sample_600k.csv
├── calidad/         # Reporte JSON de validación
├── documentacion/   # README_CSV.pdf
└── scripts/         # Scripts reproducibles de preparation

02_fase2_gcp/
├── scripts/
├── evidencias/
└── configuracion/

03_fase3_arquitectura/
04_fase4_procesamiento/ # 4 notebooks, contrato común y validador
05_fase5_hadoop/
06_fase6_procesamiento/ # 4 notebooks ejecutados, 40 consultas, validador
06_fase6_analisis/
07_fase7_entregables/
results/fase4/        # 40 consultas, CSV/JSON y tiempos por motor
results/fase5/        # salidas MapReduce copiadas de HDFS
reports/              # Informes de las Fases 4 y 5
```

## Fase 1 — Dataset

La muestra fue generada mediante un proceso de streaming en bloques, con `random_state=42`, preservando las 34 clases. El reporte de calidad conserva la distribución, nulos y duplicados.

Para regenerar la muestra desde la raíz del proyecto:

```powershell
python .\01_fase1_datos\scripts\prepare_cic_iot_2023.py
```

El script utiliza por defecto:

- Entrada: `01_fase1_datos/raw/`
- Salida: `01_fase1_datos/muestra/CICIoT2023_sample_600k.csv`
- Reporte: `01_fase1_datos/calidad/CICIoT2023_sample_600k_report.json`

## Fase 4 — Procesamiento local

Cuatro notebooks Jupyter (Polars, Dask, Modin y Spark), con diez operaciones
cada uno. Instrucciones, dependencias y verificación reproducible en
`04_fase4_procesamiento/README.md`; resultados y tiempos en `results/fase4/`,
resumen en `reports/fase4_resumen.md`.

## Fase 5 — Hadoop / Dataproc

Un clúster efímero con HDFS y tres programas del JAR de ejemplos de Hadoop:
`secondarysort`, `terasort` y `pi`. Instrucciones reproducibles, salidas,
verificación y evidencias en `05_fase5_hadoop/README.md` y
`reports/fase5_resumen.md`.

## Fase 6 — Procesamiento distribuido

Cuatro notebooks (Polars, Dask, Modin y Spark local), diez consultas idénticas
por motor sobre la muestra intacta. Resultados, tiempos y validación cruzada en
`06_fase6_procesamiento/README.md` y `06_fase6_procesamiento/results/`.

## Estructura prevista en Cloud Storage

```text
raw/       # dataset original
processed/ # muestra y archivos procesados
results/   # resultados de Polars, Dask, Modin, Spark y MapReduce
reports/   # reportes de calidad y análisis
```

## Seguridad

Las credenciales de GCP no se guardan dentro de este proyecto. La configuración de la CLI está fuera del proyecto, en:

```text
C:\Users\steve\.config\gcloud
```

No copiar `credentials.db`, `access_tokens.db` ni `application_default_credentials.json` a las carpetas del proyecto.
