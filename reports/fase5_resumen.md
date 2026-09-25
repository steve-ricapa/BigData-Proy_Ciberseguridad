# Fase 5 — Hadoop / Dataproc

## Alcance

Se procesó en Dataproc la muestra de CIC-IoT-2023 mediante tres ejemplos
MapReduce del JAR incluido en Hadoop 3.3.6, sin `wordcount`, `grep` ni `join`.
El CSV original de 600.000 filas se copió realmente a HDFS y quedó listado en
`logs/hdfs_preparar.log` con 124.498.206 bytes. Los ordenamientos usan
599.987 filas de la misma muestra: se excluyeron 13 tasas `Rate` infinitas.

| Programa | Job ID | Entrada | Salida en HDFS | Estado |
|---|---|---|---|---|
| `secondarysort` | `1e55e00add594834a00b839dcb24004d` | protocolo y Rate entera | `/fase5/salidas/secondarysort/` | DONE / FINISHED |
| `terasort` | `2e83644f3a1a41148894b6b0d38a7252` | 100 bytes/registro: clave Rate + Label/protocolo | `/fase5/salidas/terasort/` | DONE / FINISHED |
| `pi` (`QuasiMonteCarlo`) | `26894b416c8940e1aaaefd44428c1d80` | 4 maps × 10.000 muestras, sin CSV | `/fase5/salidas/pi/resultado.txt` (guardado desde el log del driver) | DONE / FINISHED |

`pi` estimó **3,1414**. `validar_resultados.py` comprueba las tres partes
de cada ordenamiento: 599.987 filas para ambos, orden correcto de las claves,
100 bytes por registro TeraSort, marcadores `_SUCCESS` y el valor de π.
`resultados/manifiesto.json` guarda tamaños y SHA-256 de los diez archivos
exportados de HDFS.

El CSV está íntegro en HDFS, pero los ejemplos tienen formatos de entrada
específicos. `secondarysort` produce pares de enteros por protocolo; `terasort`
ordena registros binarios construidos de esos flujos, **no el CSV completo**.
El trabajo `pi` mide cálculo MapReduce y no analiza ataques. No se debe
interpretar ninguno como un modelo de detección de ciberataques.

La exploración `hadoop fs -ls -R`, `-du -h` y `-head` está en
`05_fase5_hadoop/logs/hdfs_inspeccionar.log`. Los resultados también están en
`results/fase5/mapreduce/` y en el bucket bajo `results/fase5/mapreduce/`;
los registros y metadatos se conservan en `05_fase5_hadoop/logs/` y GCS.

## Infraestructura y costo

Clúster efímero `ciciot-fase5`, `us-central1-a`, imagen `2.2-debian12`,
un master y dos workers `e2-standard-2`. La configuración real está en
`logs/cluster_describe.json`. Se establecieron `--max-idle=45m` y
`--max-age=3h` para evitar que permaneciera activo. SSH por IAP no funcionó,
por lo que los comandos HDFS se ejecutaron mediante un job auxiliar de
Dataproc, distinto de los tres programas MapReduce evaluados.

Antes de eliminar el clúster se comprobó que **los diez archivos HDFS**
estaban en GCS y en el repositorio, con tamaños coincidentes y manifiesto
SHA-256; también se comprobaron en ambos sitios los logs, los metadatos JSON
de los tres jobs y la configuración. El commit `1598f56` se publicó antes del
borrado. La eliminación consta en `logs/cluster_delete.log` y un listado
posterior devolvió **cero clústeres** en la región (véase
`logs/cluster_deleted_verified.txt`).

La API Dataproc estaba deshabilitada y la cuenta de servicio carecía al
principio de `roles/dataproc.worker`: se guardaron los intentos fallidos
en `logs/` para trazabilidad; no se crearon VMs en esos intentos.
