"""Prepara entradas de dos ejemplos Hadoop a partir de la muestra CIC-IoT-2023.

TeraSort requiere registros binarios de exactamente 100 bytes: clave de 10
bytes (Rate redondeada al entero más próximo) y 90 bytes de payload. El CSV
original también se copia a HDFS sin transformarlo.
"""

import csv
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
ORIGEN = ROOT / "01_fase1_datos/muestra/CICIoT2023_sample_600k.csv"
SALIDA = ROOT / "05_fase5_hadoop/mapreduce/entrada_generada"


def main():
    SALIDA.mkdir(parents=True, exist_ok=True)
    correctas = 0
    invalidas = 0
    with ORIGEN.open(encoding="utf-8", newline="") as entrada, (
        SALIDA / "secondarysort.txt"
    ).open("w", encoding="ascii", newline="\n") as secundario, (
        SALIDA / "terasort.bin"
    ).open("wb") as terasort:
        lector = csv.DictReader(entrada)
        for fila in lector:
            rate = float(fila["Rate"])
            if not math.isfinite(rate):
                invalidas += 1
                continue
            protocolo = int(float(fila["Protocol Type"]))
            # Dos enteros de 32 bits, como requiere SecondarySort.MapClass.
            valor = round(rate)
            assert 0 <= valor < 2**31
            secundario.write(f"{protocolo} {valor}\n")

            # TeraInputFormat lee registros contiguos de 100 bytes, sin LF.
            clave = f"{valor:010d}".encode("ascii")
            resto = f"{fila['Label']}|{protocolo}".encode("ascii")[:90]
            terasort.write(clave + resto.ljust(90, b" "))
            correctas += 1
    assert correctas == 599_987 and invalidas == 13, (correctas, invalidas)
    assert (SALIDA / "terasort.bin").stat().st_size == correctas * 100
    print(f"Registros válidos: {correctas}; Rate infinita: {invalidas}")
    print(f"Salida: {SALIDA}")


if __name__ == "__main__":
    main()
