# Fase 6 — Procesamiento distribuido: 10 consultas × 4 motores

Se ejecutaron las **mismas diez consultas** sobre la muestra intacta
`../01_fase1_datos/muestra/CICIoT2023_sample_600k.csv` (600.000 filas,
40 columnas, 118,7 MiB). No se alteraron ni el CSV de origen ni las fases
anteriores. Los cuatro notebooks contienen el código completo de sus diez
consultas y sus salidas ejecutadas:

| Motor | Notebook | Ejecución |
|---|---|---|
| Polars | `polars/polars_consultas.ipynb` | CPU local |
| Dask | `dask/dask_consultas.ipynb` | Particiones de 16 MB, 4 hilos locales |
| Modin | `modin/modin_consultas.ipynb` | Backend Dask local |
| Spark | `spark/spark_consultas.ipynb` | PySpark `local[4]`, Java 21 |

## Consultas y requisitos cubiertos

| ID | Consulta exacta | Requisitos del curso | Salida por motor |
|---|---|---|---|
| Q1 | Validación general: filas, columnas, nulos, infinitos, duplicados | Validación, limpieza (diagnóstico previo) | `q01_validacion.csv` |
| Q2 | Deduplicación exacta lógica: antes, después, reducción | Duplicados, limpieza | `q02_duplicados.csv` |
| Q3 | `Attack_Family` desde `Label`; 34 etiquetas mapeadas | Transformaciones, transformación de variables, agrupación | `q03_familias.csv` |
| Q4 | `Label != Benign`, cantidad y porcentaje | Filtrado | `q04_malicioso.csv` |
| Q5 | `Traffic_Type`: benigno vs malicioso | Transformación, agregaciones, ordenamiento | `q05_tipo_trafico.csv` |
| Q6 | Frecuencia/porcentaje por familia, orden decreciente | Agrupaciones, agregaciones, ordenamiento | `q06_distribucion_familias.csv` |
| Q7 | Top 10 **ataques** por etiqueta; % sobre 600.000 | Filtrado, agrupaciones, ordenamiento | `q07_top10_ataques.csv` |
| Q8 | Rate/IAT por familia: media, mediana, mínimo y máximo | Limpieza puntual, agrupaciones, métricas | `q08_rate_iat_familia.csv` |
| Q9 | `Protocol Type` mapeado a 5 nombres, distribución **dentro** de familia | Transformación, agrupaciones, métricas | `q09_protocolos_familia.csv` |
| Q10 | Perfil benigno/malicioso: Rate, IAT, ACK/SYN, `Tot sum`, `AVG` | Transformación, agrupaciones, métricas | `q10_perfil_trafico.csv` |

Los CSV y `tiempos.csv` se encuentran bajo `results/<motor>/`.
`results/comparativo_tiempos.csv` consolida los diez tiempos; `logs/validacion.txt`
conserva el chequeo de consistencia. Las fuentes editables de cada notebook
están en `<motor>/consultas.py`; `generar_notebooks.py` sincroniza el código
sin eliminar las salidas de celdas no modificadas. Volver a ejecutar tras
una regeneración para que los tiempos y las evidencias sean nuevos.

## Política de datos y comparabilidad

- Q1 y Q2 comparan los **40 campos tal como aparecen en el CSV**. Dask y Modin
  leen adicionalmente los campos como texto para deduplicar; sus lectores
  numéricos fusionaban **un registro distinto** debido al redondeo binario
  del parser. Todos dan **131.030 duplicados** sin redondear la muestra.
- Q3 usa un diccionario exhaustivo de las 34 etiquetas → 8 familias; no
  infiere la familia solo por el prefijo (por ejemplo `VulnerabilityScan` es Recon).
- Q7 **excluye `Benign`** y usa 600.000 como denominador de porcentajes.
- Q8 y Q10 convierten **únicamente los 13 `Rate` infinitos** en NULL/NaN,
  dentro de esas consultas. Las medias/medianas/extremos de Rate usan
  599.987 filas válidas; IAT, flags y tamaño mantienen 600.000.
  Los 9 nulos de Std/Variance se reportan en Q1 (18 celdas nulas), sin
  eliminarlos del resto. Q2 trabaja sobre las filas originales, sin limpiar.
- Q9 usa el número IP `Protocol Type`: `0=HOPOPT`, `1=ICMP`, `6=TCP`,
  `17=UDP`, `47=GRE`. No usa las proporciones `TCP`/`UDP`/`ICMP`.
- Spark lee `Rate` inicialmente como texto: su lector `DoubleType` convertiría
  `inf` a NULL y falsearía Q1. Se convierte explícitamente a double/`+inf`.
- Las medianas son **exactas** en los cuatro motores: Dask reagrupa por familia
  para calcular la mediana en pandas por grupo, no usa cuantiles aproximados.
- Todas las consultas trabajan con las 600.000 filas originales salvo Q2
  (versión lógica deduplicada); Q4/Q7 (filtros específicos); Q8/Q10 (solo
  tratamiento puntual de Rate). Las salidas tabulares agrupadas son las
  únicas que pasan al driver para convertirse en CSV.
- Los tiempos corresponden a la consulta materializada, **excluyen** lectura
  inicial y serialización del CSV de salida; Q1/Q2 incluyen deduplicación.
  Spark usa caché del dataframe leído, Dask no; por eso **no es un benchmark
  neutral**. No se interpretan las diferencias como superioridad intrínseca.

## Resultados principales

- 600.000 filas, 40 columnas, **18 nulos, 13 infinitos y 131.030 duplicados**;
  468.970 filas distintas (reducción 21,838%).
- 14.087 benignas (2,348%) y 585.913 maliciosas (97,652%).
- 8 familias; DDoS es la más común (435.902, 72,650%).
- Mayor tipo de ataque: `DDoS-ICMP_Flood` (92.356, 15,393% del total).
- Los resultados de **las 40 consultas coinciden** entre motores con tolerancia
  numérica de 1e-6 absoluta o 1e-9 relativa, la mayor de ambas; Q1–Q7,
  Q9 y las columnas discretas de Q8/Q10 coinciden exactamente.

## Tiempos medidos (segundos)

| Consulta | Polars | Dask | Modin | Spark |
|---|---:|---:|---:|---:|
| Q1 | 0,430 | 19,634 | 189,473 | 13,534 |
| Q2 | 0,387 | 16,031 | 86,993 | 2,523 |
| Q3 | 0,094 | 5,930 | 32,449 | 5,094 |
| Q4 | 0,017 | 1,846 | 13,929 | 0,600 |
| Q5 | 0,025 | 2,022 | 21,811 | 3,622 |
| Q6 | 0,013 | 1,893 | 22,496 | 0,705 |
| Q7 | 0,050 | 1,839 | 36,890 | 0,707 |
| Q8 | 0,058 | 5,536 | 26,362 | 4,139 |
| Q9 | 0,085 | 2,092 | 46,510 | 1,820 |
| Q10 | 0,070 | 8,374 | 26,547 | 3,483 |
| **Suma Q1–Q10** | **1,229** | **65,197** | **503,460** | **36,227** |

Entorno: **Windows 11, procesador AMD de 16 hilos lógicos, Python 3.12**.
Polars local; Dask `threads` 4; Modin con Dask local; Spark `local[4]`,
Java 21 y 8 particiones shuffle. Se midió **una única ejecución**, sin
warm-up ni intervalos de confianza; Spark/Modin tienen costes de arranque,
cache, planificación y materialización distintos. No se usó Dataproc ni GCS
en esta fase: la copia local es idéntica a la muestra del bucket y evita
gastos de cómputo innecesarios.

## Cómo reproducir (PowerShell, desde la raíz del repo)

Reusar el entorno de la Fase 4 (Python 3.12, Java 21):

```powershell
.\.venv\Scripts\python.exe -m pip install -r .\04_fase4_procesamiento\requirements.txt
.\.venv\Scripts\python.exe -X utf8 .\06_fase6_procesamiento\generar_notebooks.py
foreach ($m in @('polars','dask','modin','spark')) {
  .\.venv\Scripts\python.exe -X utf8 -m nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1200 "06_fase6_procesamiento/$m/${m}_consultas.ipynb"
  if ($LASTEXITCODE -ne 0) { throw "Falló el notebook $m" }
}
.\.venv\Scripts\python.exe -X utf8 .\06_fase6_procesamiento\validar_resultados.py
.\.venv\Scripts\python.exe -X utf8 .\06_fase6_procesamiento\consolidar_tiempos.py
```

También se pueden abrir los cuatro notebooks con kernel del `.venv` y ejecutar
**Run All** desde Jupyter. El arranque de Spark en Windows avisa de la ausencia
de `winutils.exe`, pero los resultados se escriben desde el driver en CSV
(no se usa `Spark.write` local) y los cuatro notebooks terminan correctamente.

## Límites de interpretación

`Tot size` = `AVG` y `IPv` = `LLC` exactamente; `Variance` es derivable de
`Std`. `Time_To_Live` **no** mide duración. Faltan IP, puertos, timestamps y
`flow_duration`. `Number` está saturado a 100 en la mayoría de los flujos.
Hay fuerte desbalance (2,35% benignas): estas consultas son descriptivas y
de procesamiento, **no validan un detector de ataques ni identifican equipos**.
