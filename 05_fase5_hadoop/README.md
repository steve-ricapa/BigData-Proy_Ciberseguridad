# Fase 5 — Hadoop MapReduce en Dataproc

Se creó el clúster temporal `ciciot-fase5` en `us-central1-a`, con imagen
`2.2-debian12` (Hadoop 3.3.6), un master y dos workers `e2-standard-2`.
Los tres programas **del JAR de ejemplos** ejecutados fueron `secondarysort`,
`terasort` y `pi` (implementación `QuasiMonteCarlo`). Ninguno es `wordcount`,
`grep` ni `join`. Los tres estados de los jobs son `DONE` y sus aplicaciones
YARN figuran como `FINISHED` en `logs/job_*.json` y `logs/job_*.log`.

> Corrección necesaria del plan inicial: Hadoop 3.3.6 **no incluye** el
> ejemplo `stats` ni la clase `PiEstimation`; el usuario autorizó reemplazar
> `stats` por `secondarysort`, manteniendo el cálculo de π. `terasort` no lee
> CSV arbitrarios: necesita registros de 100 bytes.

## Entrada y resultados

- CSV original (600.000 filas) copiado desde el bucket a
  `hdfs:///fase5/entrada/csv/CICIoT2023_sample_600k.csv`.
- `mapreduce/preparar_entradas.py` deriva del mismo CSV las entradas de
  `secondarysort` (`Protocol Type`, `Rate` redondeada al entero) y `terasort`
  (clave de 10 bytes con `Rate` redondeada; 90 bytes con `Label` y protocolo).
  Descarta las 13 filas con `Rate` infinita: quedan **599.987** entradas.
- Las salidas residen en `hdfs:///fase5/salidas/{secondarysort,terasort,pi}/`;
  se exploraron con `hadoop fs -ls -R`, `-du -h`, `-head` y `-cat`. Los tres
  `part-r-*` de `terasort` son binarios (100 bytes por registro).
- El ejemplo `pi` imprime su resultado en el **driver**, no conserva por sí
  mismo un `part-r-*` en HDFS. Su valor, parámetros y job ID se extrajeron del
  log y se guardaron explícitamente como `hdfs:///fase5/salidas/pi/resultado.txt`.
- Copia de las salidas de HDFS: `results/fase5/mapreduce/` local y
  `gs://ciciot2023-bigdata-utec-proyecto1-gm/results/fase5/mapreduce/`.
  El manifiesto de tamaños y SHA-256 está en `resultados/manifiesto.json`.
- Logs, configuración del clúster, metadatos de cada job y exploración HDFS:
  `logs/` local y `gs://.../results/fase5/evidencias/logs/`.

**Estado final:** el clúster se eliminó después de verificar las copias en
GCS y GitHub (`1598f56`); `logs/cluster_delete.log` conserva la respuesta
de Dataproc y `logs/cluster_deleted_verified.txt` documenta que ya no
figura en la lista de clústeres de `us-central1`.

## Reproducir (PowerShell en la raíz del repositorio)

Se necesitan `gcloud`, una cuenta con permisos para Dataproc y un principal
de ejecución con `roles/dataproc.worker` en el proyecto y acceso de objetos
al bucket. Se usa `gcloud.cmd` para pasar los argumentos después de `--`
correctamente desde Windows PowerShell. No depende de SSH: el acceso por IAP
del proyecto no dejó conectar; `hdfs_admin.py` se ejecuta como job auxiliar
de Dataproc únicamente para operar HDFS desde el master.

```powershell
$env:CLOUDSDK_CONFIG = 'C:\Users\steve\.config\gcloud'
$p = 'bigdata-proyecto1-ciciot2023'
$b = 'gs://ciciot2023-bigdata-utec-proyecto1-gm'
$jar = 'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar'

.\.venv\Scripts\python.exe -X utf8 .\05_fase5_hadoop\mapreduce\preparar_entradas.py
gcloud services enable dataproc.googleapis.com --project=$p
gcloud storage cp .\05_fase5_hadoop\mapreduce\entrada_generada\secondarysort.txt "$b/processed/fase5/"
gcloud storage cp .\05_fase5_hadoop\mapreduce\entrada_generada\terasort.bin "$b/processed/fase5/"
gcloud storage cp .\05_fase5_hadoop\mapreduce\hdfs_admin.py "$b/processed/fase5/"

gcloud dataproc clusters create ciciot-fase5 --project=$p --region=us-central1 --zone=us-central1-a --image-version=2.2-debian12 --master-machine-type=e2-standard-2 --worker-machine-type=e2-standard-2 --num-workers=2 --master-boot-disk-size=100 --worker-boot-disk-size=100 --master-boot-disk-type=pd-balanced --worker-boot-disk-type=pd-balanced --max-idle=45m --max-age=3h
gcloud.cmd dataproc jobs submit pyspark "$b/processed/fase5/hdfs_admin.py" --project=$p --region=us-central1 --cluster=ciciot-fase5 -- preparar
gcloud.cmd dataproc jobs submit hadoop --project=$p --region=us-central1 --cluster=ciciot-fase5 --jar=$jar -- secondarysort /fase5/entrada/secondarysort /fase5/salidas/secondarysort
gcloud.cmd dataproc jobs submit hadoop --project=$p --region=us-central1 --cluster=ciciot-fase5 --jar=$jar -- terasort /fase5/entrada/terasort /fase5/salidas/terasort
gcloud.cmd dataproc jobs submit hadoop --project=$p --region=us-central1 --cluster=ciciot-fase5 --jar=$jar -- pi 4 10000
```

Tras el último job, extraer `Estimated value of Pi is ...` del log como en
`resultados/pi_resultado.txt`, subirlo a `$b/processed/fase5/pi_resultado.txt`
y ejecutar el auxiliar con `-- pi_hdfs`, `-- inspeccionar`, `-- exportar` y
`-- marcadores`, **en ese orden**. Guardar los logs de cada comando, `gcloud
dataproc clusters describe` y `gcloud dataproc jobs describe` (IDs concretos
en `logs/job_*.json`). Descargar `results/fase5/mapreduce/` desde GCS y
verificar con:

```powershell
.\.venv\Scripts\python.exe -X utf8 .\05_fase5_hadoop\validar_resultados.py
```

**No borrar el clúster hasta que** los resultados, el manifiesto, los logs,
la configuración y los JSON de los tres jobs existan y se comprueben tanto
en GCS como en el repositorio. Guardar después la confirmación del borrado:

```powershell
gcloud dataproc clusters delete ciciot-fase5 --project=$p --region=us-central1 --quiet
gcloud dataproc clusters list --project=$p --region=us-central1
```
