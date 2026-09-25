# BigData-Proy_Ciberseguridad

Proyecto universitario para el análisis distribuido del dataset CIC-IoT-2023 en Google Cloud.

## Estado actual

- **Fases completadas:** Fase 1 — Dataset; Fase 2 — Configuración de GCP; Fase 3 — Diseño de arquitectura.
- **Fase actual:** Fase 4 — Procesamiento distribuido (pendiente).
- **Documento de arquitectura:** `03_fase3_arquitectura/documentacion/arquitectura_big_data.md`
- **Proyecto GCP:** `bigdata-proyecto1-ciciot2023`
- **Bucket:** `gs://ciciot2023-bigdata-utec-proyecto1-gm/`
- **Región del bucket:** `US-CENTRAL1`
- **Muestra local:** 600,000 registros y 40 columnas.
- **Dataset original local:** 309 CSV, 34 clases y 46,776,700 registros.
- **Dataproc:** todavía no creado; la API de Dataproc permanece deshabilitada.
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
04_fase4_procesamiento/
05_fase5_hadoop/
06_fase6_analisis/
07_fase7_entregables/
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
