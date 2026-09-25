# Fase 4 — Procesamiento con cuatro motores

Entregable: **40 operaciones**, diez por motor, en notebooks Jupyter con el
código de cada consulta en sus celdas:

| Motor | Notebook |
|---|---|
| Polars | `polars/operaciones_polars.ipynb` |
| Dask | `dask/operaciones_dask.ipynb` |
| Modin | `modin/operaciones_modin.ipynb` |
| Spark | `spark/operaciones_spark.ipynb` |

Las operaciones cubren validación, limpieza, duplicados, transformación,
filtrado, agregaciones, agrupaciones, ordenamiento, métricas y resumen por
clase. El detalle del contrato común figura en `comun/catalogo_operaciones.py`.
La muestra de entrada es `../01_fase1_datos/muestra/CICIoT2023_sample_600k.csv`.

## Ejecutar localmente

Desde la raíz del repositorio, con Python 3.12 y Java 21 instalados:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r .\04_fase4_procesamiento\requirements.txt
.\.venv\Scripts\python.exe -m ipykernel install --user --name fase4 --display-name "Python (fase4)"
.\.venv\Scripts\jupyter-lab.exe
```

Abrir cada notebook con el kernel `fase4` y ejecutar **Run All**. Cada notebook
lee el CSV local, aplica sus diez operaciones y deja 13 CSV, dos JSON y archivos
de tiempos en `../results/fase4/<motor>/`. Los cuatro se probaron con
`local[*]` para Spark, Dask con cuatro hilos y Modin con su backend configurado
en el notebook. En Windows, ejecutar Spark con el Python del venv; el notebook
configura `PYSPARK_PYTHON` con el intérprete en uso. Spark guarda los resultados
pequeños mediante el formateador común; no necesita Dataproc para esta fase.

Si se modifican la muestra o `comun/umbrales.json`, recalcular los percentiles
antes de volver a ejecutar los notebooks:

```powershell
.\.venv\Scripts\python.exe .\04_fase4_procesamiento\comun\generar_umbrales.py
```

Verificar resultados y tiempos después de las cuatro ejecuciones:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\04_fase4_procesamiento\validar_resultados.py
```

El validador exige los 13 CSV y dos JSON previstos por motor, comprueba
los tiempos de los once pasos (carga + diez operaciones) y compara las celdas
de resultados con tolerancia para redondeo de coma flotante: **1e-6 absoluta o
1e-9 relativa**, lo que sea mayor. No se comparan tiempos: dependen del equipo.
La muestra de transformación se alinea por las cinco columnas de origen.

## Artefactos

- `../results/fase4/<motor>/`: salidas del procesamiento, también en
  `gs://ciciot2023-bigdata-utec-proyecto1-gm/results/fase4/`.
- `../reports/fase4_resumen.md`: cobertura, datos obtenidos y limitaciones.
- `../reports/fase4_tiempos.csv`: tiempos por operación y motor.

Los CSV no reemplazan la muestra original ni son una base de datos o vistas SQL.
En la Fase 6 se interpretarán los resultados y se plantearán las preguntas
analíticas; si alguna requiere granularidad adicional, se consultará la muestra
original. No se crea Dataproc en esta fase.
