"""Verifica las salidas descargadas desde HDFS y crea manifiesto SHA-256."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "results/fase5/mapreduce"
MANIFIESTO = ROOT / "05_fase5_hadoop/resultados/manifiesto.json"


def main():
    esperado = 599_987
    total_secundario = 0
    total_tera = 0
    archivos = []
    for trabajo in ("secondarysort", "terasort"):
        partes = sorted((BASE / trabajo).glob("part-r-*"))
        assert len(partes) == 3, (trabajo, partes)
        assert (BASE / trabajo / "_SUCCESS").exists()
        if trabajo == "secondarysort":
            for parte in partes:
                ultimo = None
                with parte.open(encoding="utf-8") as f:
                    for linea in f:
                        linea = linea.strip()
                        if linea.startswith("-"):
                            continue
                        protocolo, rate = map(int, linea.split())
                        par = (protocolo, rate)
                        assert ultimo is None or ultimo <= par, (ultimo, par)
                        ultimo = par
                        total_secundario += 1
        else:
            assert (BASE / trabajo / "_partition.lst").exists()
            ultimo = None
            for parte in partes:
                with parte.open("rb") as f:
                    while bloque := f.read(100):
                        assert len(bloque) == 100
                        clave = bloque[:10]
                        assert clave.isdigit() and (ultimo is None or ultimo <= clave)
                        ultimo = clave
                        total_tera += 1
    assert total_secundario == total_tera == esperado, (total_secundario, total_tera)
    pi = (BASE / "pi/resultado.txt").read_text(encoding="utf-8")
    assert "pi_estimada=3.14140000000000000000" in pi
    for p in sorted(BASE.rglob("*")):
        if not p.is_file():
            continue
        with p.open("rb") as f:
            h = hashlib.file_digest(f, "sha256").hexdigest()
        archivos.append({"ruta": p.relative_to(ROOT).as_posix(),
                         "bytes": p.stat().st_size, "sha256": h})
    assert len(archivos) == 10
    salida = {"registros_secondarysort": total_secundario,
              "registros_terasort": total_tera, "pi": 3.1414,
              "archivos": archivos}
    MANIFIESTO.write_text(json.dumps(salida, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"OK: {len(archivos)} archivos; {esperado} registros por ordenamiento; π = 3.1414")


if __name__ == "__main__":
    main()
