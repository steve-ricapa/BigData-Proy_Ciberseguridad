"""Operaciones HDFS desde el driver de Dataproc cuando no hay acceso SSH.

Uso: gcloud dataproc jobs submit pyspark gs://.../hdfs_admin.py -- preparar|inspeccionar|exportar
No es un programa MapReduce; solo orquesta hadoop fs en el master.
"""

import glob
import os
import subprocess
import sys

BUCKET = "gs://ciciot2023-bigdata-utec-proyecto1-gm"
INPUT = BUCKET + "/processed/fase5"
OUT = BUCKET + "/results/fase5/mapreduce"


def run(*args):
    print("$", *args, flush=True)
    subprocess.run(args, check=True, timeout=900)


def fs(*args):
    run("hadoop", "fs", *args)


def main():
    modo = sys.argv[1]
    jars = glob.glob("/usr/lib/hadoop-mapreduce/*examples*.jar")
    jars += glob.glob("/opt/hadoop/share/hadoop/mapreduce/*examples*.jar")
    print("HOST=", os.uname().nodename, "JAR=", jars, flush=True)
    run("hadoop", "version")
    if not jars:
        raise RuntimeError("No se encontró hadoop-mapreduce-examples.jar")
    if modo == "preparar":
        for carpeta in ("/fase5/entrada/csv", "/fase5/entrada/secondarysort", "/fase5/entrada/terasort", "/fase5/salidas"):
            fs("-mkdir", "-p", carpeta)
        for origen, destino in (
            (BUCKET + "/processed/CICIoT2023_sample_600k.csv", "/fase5/entrada/csv/"),
            (INPUT + "/secondarysort.txt", "/fase5/entrada/secondarysort/"),
            (INPUT + "/terasort.bin", "/fase5/entrada/terasort/"),
        ):
            fs("-cp", origen, destino)
        fs("-ls", "-R", "/fase5/entrada")
        fs("-du", "-h", "/fase5/entrada")
        fs("-head", "/fase5/entrada/secondarysort/secondarysort.txt")
    elif modo == "inspeccionar":
        fs("-ls", "-R", "/fase5")
        fs("-du", "-h", "/fase5/salidas")
        for trabajo in ("secondarysort",):
            fs("-head", f"/fase5/salidas/{trabajo}/part-r-00000")
        fs("-head", "/fase5/salidas/pi/resultado.txt")
        # TeraSort es binario (registros de 100 bytes), no mostrar con -head.
        # Los tamaños y _SUCCESS quedan en el listado previo.
    elif modo == "exportar":
        for trabajo in ("secondarysort", "terasort", "pi"):
            fs("-mkdir", "-p", OUT + f"/{trabajo}/")
            origen = f"/fase5/salidas/{trabajo}/resultado.txt" if trabajo == "pi" else f"/fase5/salidas/{trabajo}/part-*"
            fs("-cp", origen, OUT + f"/{trabajo}/")
            fs("-ls", OUT + f"/{trabajo}/")
    elif modo == "pi_hdfs":
        fs("-mkdir", "-p", "/fase5/salidas/pi")
        fs("-cp", INPUT + "/pi_resultado.txt", "/fase5/salidas/pi/resultado.txt")
        fs("-cat", "/fase5/salidas/pi/resultado.txt")
    elif modo == "marcadores":
        fs("-cp", "/fase5/salidas/secondarysort/_SUCCESS", OUT + "/secondarysort/")
        fs("-cp", "/fase5/salidas/terasort/_SUCCESS", OUT + "/terasort/")
        fs("-cp", "/fase5/salidas/terasort/_partition.lst", OUT + "/terasort/")
        fs("-ls", "-R", "/fase5/salidas")
    else:
        raise ValueError(modo)


if __name__ == "__main__":
    main()
