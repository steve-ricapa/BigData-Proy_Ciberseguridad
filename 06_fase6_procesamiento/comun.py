"""Contrato compartido de las diez consultas; no procesa filas por los motores."""

from __future__ import annotations

import csv
import math
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
FASE = RAIZ / "06_fase6_procesamiento"
ENTRADA = RAIZ / "01_fase1_datos/muestra/CICIoT2023_sample_600k.csv"
RESULTADOS = FASE / "results"
LOGS = FASE / "logs"

FAMILIAS = {
    "Benign": ["Benign"],
    "DDoS": ["DDoS-ACK_Fragmentation", "DDoS-HTTP_Flood", "DDoS-ICMP_Flood",
             "DDoS-ICMP_Fragmentation", "DDoS-PSHACK_FLOOD", "DDoS-RSTFINFLOOD",
             "DDoS-SYN_Flood", "DDoS-SlowLoris", "DDoS-SynonymousIP_Flood",
             "DDoS-TCP_Flood", "DDoS-UDP_Flood", "DDoS-UDP_Fragmentation"],
    "DoS": ["DoS-HTTP_Flood", "DoS-SYN_Flood", "DoS-TCP_Flood", "DoS-UDP_Flood"],
    "Mirai": ["Mirai-greeth_flood", "Mirai-greip_flood", "Mirai-udpplain"],
    "Recon": ["Recon-HostDiscovery", "Recon-OSScan", "Recon-PingSweep",
              "Recon-PortScan", "VulnerabilityScan"],
    "Spoofing": ["DNS_Spoofing", "MITM-ArpSpoofing"],
    "Web-based": ["Backdoor_Malware", "BrowserHijacking", "CommandInjection",
                  "SqlInjection", "Uploading_Attack", "XSS"],
    "BruteForce": ["DictionaryBruteForce"],
}
ETIQUETAS = {etiqueta: familia for familia, clases in FAMILIAS.items() for etiqueta in clases}
assert len(ETIQUETAS) == 34
PROTOCOLOS = {0: "HOPOPT", 1: "ICMP", 6: "TCP", 17: "UDP", 47: "GRE"}
NOMBRES = {
    "Q1": "q01_validacion.csv", "Q2": "q02_duplicados.csv",
    "Q3": "q03_familias.csv", "Q4": "q04_malicioso.csv",
    "Q5": "q05_tipo_trafico.csv", "Q6": "q06_distribucion_familias.csv",
    "Q7": "q07_top10_ataques.csv", "Q8": "q08_rate_iat_familia.csv",
    "Q9": "q09_protocolos_familia.csv", "Q10": "q10_perfil_trafico.csv",
}
TITULOS = {
    "Q1": "Validación general", "Q2": "Tratamiento de duplicados",
    "Q3": "Crear familia de ataque", "Q4": "Filtrado malicioso",
    "Q5": "Benigno vs malicioso", "Q6": "Distribución por familia",
    "Q7": "Top 10 tipos de ataque", "Q8": "Rate e IAT por familia",
    "Q9": "Protocolos por familia", "Q10": "Perfil benigno vs malicioso",
}


def escribir(motor: str, id_: str, columnas: list[str], filas: list[dict]) -> None:
    carpeta = RESULTADOS / motor
    carpeta.mkdir(parents=True, exist_ok=True)
    with (carpeta / NOMBRES[id_]).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columnas, lineterminator="\n")
        writer.writeheader()
        for fila in filas:
            writer.writerow({col: fila[col] for col in columnas})


def mostrar(motor: str, id_: str, filas: list[dict]) -> None:
    assert filas, f"{id_}: tabla vacía"
    escribir(motor, id_, list(filas[0]), filas)
    print(f"{id_}: {len(filas)} filas → {NOMBRES[id_]}", flush=True)
    for fila in filas[:12]:
        print(" ", fila, flush=True)
    if len(filas) > 12:
        print(f"  ... {len(filas) - 12} filas adicionales en CSV", flush=True)


def medir(id_: str, funcion, tiempos: list[dict], motor: str):
    inicio = time.perf_counter()
    resultado = funcion()
    segundos = round(time.perf_counter() - inicio, 3)
    tiempos.append({"Consulta": id_, "Tiempo": segundos})
    mostrar(motor, id_, resultado)
    print(f"{id_}: {segundos:.3f} s (incluye cómputo/materialización, no exportación)\n", flush=True)
    return resultado


def cerrar(motor: str, tiempos: list[dict]) -> None:
    assert [r["Consulta"] for r in tiempos] == [f"Q{i}" for i in range(1, 11)]
    escribir(motor, "TIEMPOS", ["Consulta", "Tiempo"], tiempos)
    print("Tiempos medidos:", tiempos, flush=True)


NOMBRES["TIEMPOS"] = "tiempos.csv"


def convertir(valor):
    """Convertir escalares numpy/Arrow a Python manteniendo nulos."""
    if hasattr(valor, "item"):
        valor = valor.item()
    if isinstance(valor, float) and not math.isfinite(valor):
        return None
    return valor


def filas_tabla(tabla):
    """Solo se usa sobre resultados agrupados (nunca sobre las 600k filas)."""
    if hasattr(tabla, "to_dicts"):
        return tabla.to_dicts()
    if hasattr(tabla, "to_dict"):
        return [{k: convertir(v) for k, v in r.items()}
                for r in tabla.to_dict("records")]
    return [{k: convertir(v) for k, v in r.asDict().items()}
            for r in tabla.collect()]
