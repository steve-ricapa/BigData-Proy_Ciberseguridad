from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from itertools import islice
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_SAMPLE_SIZE = 600_000
DEFAULT_SEED = 42
DEFAULT_LINES_PER_BLOCK = 100_000
DEFAULT_COUNT_BUFFER_BYTES = 8 * 1024 * 1024
DEFAULT_OUTPUT_ROWS_PER_BUFFER = 10_000
DEFAULT_OUTPUT_BUFFER_BYTES = 4 * 1024 * 1024
LABEL_COLUMN = "Label"
NULL_MARKERS = {
    b"",
    b"NA",
    b"N/A",
    b"NULL",
    b"null",
    b"NaN",
    b"nan",
}


@dataclass
class SourceFile:
    path: Path
    relative_path: str
    label: str
    row_count: int = 0
    sample_count: int = 0
    header: bytes | None = None


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parents[1]
    data_root = project_root / "01_fase1_datos"
    parser = argparse.ArgumentParser(
        description="Muestreo estratificado y de baja memoria para CIC-IoT-2023."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=data_root / "raw",
        help="Carpeta que contiene las carpetas de clases.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=data_root / "muestra" / "CICIoT2023_sample_600k.csv",
        help="Ruta del CSV final.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=data_root / "calidad" / "CICIoT2023_sample_600k_report.json",
        help="Ruta del reporte JSON.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=f"Filas de datos requeridas (por defecto: {DEFAULT_SAMPLE_SIZE}).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Semilla reproducible (por defecto: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--lines-per-block",
        type=int,
        default=DEFAULT_LINES_PER_BLOCK,
        help=(
            "Líneas procesadas por bloque durante la escritura "
            f"(por defecto: {DEFAULT_LINES_PER_BLOCK})."
        ),
    )
    return parser.parse_args()


def label_from_directory(directory_name: str) -> str:
    return "Benign" if directory_name == "Benign_Final" else directory_name


def discover_source_files(input_dir: Path, output_path: Path) -> list[SourceFile]:
    input_dir = input_dir.resolve()
    output_resolved = output_path.resolve()
    files: list[SourceFile] = []

    for path in input_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() != ".csv":
            continue
        if path.resolve() == output_resolved:
            continue

        relative_path = path.relative_to(input_dir).as_posix()
        label = label_from_directory(path.parent.name)
        if not label or "," in label or "\n" in label or "\r" in label:
            raise ValueError(f"Nombre de clase no utilizable: {label!r}")
        files.append(SourceFile(path=path, relative_path=relative_path, label=label))

    files.sort(key=lambda item: (item.label.casefold(), item.relative_path.casefold()))
    if not files:
        raise FileNotFoundError(f"No se encontraron archivos CSV en {input_dir}")
    return files


def strip_line_ending(line: bytes) -> bytes:
    if line.endswith(b"\r\n"):
        return line[:-2]
    if line.endswith((b"\n", b"\r")):
        return line[:-1]
    return line


def read_and_validate_header(path: Path) -> bytes:
    with path.open("rb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as handle:
        raw_header = handle.readline()
    if not raw_header:
        raise ValueError(f"Archivo CSV vacío: {path}")

    header = strip_line_ending(raw_header)
    columns = header.decode("utf-8-sig").split(",")
    if len(columns) != len(set(columns)):
        raise ValueError(f"Cabecera con columnas duplicadas en {path}")
    if LABEL_COLUMN in columns:
        raise ValueError(
            f"La entrada ya contiene una columna {LABEL_COLUMN!r}; se esperaba una etiqueta "
            f"derivada de la carpeta: {path}"
        )
    return header


def count_data_rows(path: Path, header_size: int) -> int:
    file_size = path.stat().st_size
    data_size = file_size - header_size
    if data_size <= 0:
        return 0

    newline_count = 0
    last_byte: int | None = None
    with path.open("rb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as handle:
        handle.seek(header_size)
        while block := handle.read(DEFAULT_COUNT_BUFFER_BYTES):
            newline_count += block.count(b"\n")
            last_byte = block[-1]

    # Si el último registro no termina en salto de línea, falta contabilizarlo.
    if last_byte is not None and last_byte != ord("\n"):
        newline_count += 1
    return newline_count


def scan_sources(files: Sequence[SourceFile]) -> tuple[bytes, list[str]]:
    print("[1/5] Contando registros por archivo y clase...", flush=True)
    expected_header: bytes | None = None
    expected_columns: list[str] | None = None
    processed_bytes = 0
    total_rows = 0
    started = time.perf_counter()

    for index, source in enumerate(files, start=1):
        header = read_and_validate_header(source.path)
        columns = header.decode("utf-8-sig").split(",")
        if expected_header is None:
            expected_header = header
            expected_columns = columns
        elif header != expected_header:
            raise ValueError(
                "Cabeceras inconsistentes entre archivos CIC-IoT-2023:\n"
                f"  Esperada: {expected_header!r}\n"
                f"  Encontrada en {source.relative_path}: {header!r}"
            )

        # El tamaño del salto de línea forma parte del offset, pero no del header.
        with source.path.open("rb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as handle:
            raw_header_size = len(handle.readline())
        source.header = header
        source.row_count = count_data_rows(source.path, raw_header_size)

        total_rows += source.row_count
        processed_bytes += source.path.stat().st_size

        if index == 1 or index % 20 == 0 or index == len(files):
            elapsed = max(time.perf_counter() - started, 1e-9)
            print(
                f"      {index:>3}/{len(files)} archivos | "
                f"{total_rows:,} filas | {processed_bytes / (1024**3):.2f} GiB | "
                f"{processed_bytes / elapsed / (1024**2):.1f} MiB/s",
                flush=True,
            )

    if expected_header is None or expected_columns is None:
        raise ValueError("No se pudo determinar la cabecera del dataset")
    if total_rows == 0:
        raise ValueError("Los CSV no contienen filas de datos")
    return expected_header, expected_columns


def largest_remainder_allocation(
    weights: Sequence[int],
    capacities: Sequence[int],
    target: int,
    minimums: Sequence[int] | None = None,
    tie_keys: Sequence[str] | None = None,
) -> list[int]:
    """Asigna ``target`` unidades de forma proporcional, respetando capacidades."""
    size = len(weights)
    if not (len(capacities) == size and (minimums is None or len(minimums) == size)):
        raise ValueError("Arreglos de entrada con longitudes inconsistentes")
    if tie_keys is None:
        tie_keys = [str(index) for index in range(size)]
    if len(tie_keys) != size:
        raise ValueError("Tie keys inconsistentes")
    if any(weight < 0 for weight in weights):
        raise ValueError("Los pesos no pueden ser negativos")
    if any(capacity < 0 for capacity in capacities):
        raise ValueError("Las capacidades no pueden ser negativas")

    allocation = [0 if minimums is None else value for value in minimums]
    if any(value < 0 for value in allocation):
        raise ValueError("Los mínimos no pueden ser negativos")
    if any(need > capacity for need, capacity in zip(allocation, capacities)):
        raise ValueError("El mínimo supera la capacidad de una categoría")
    if sum(allocation) > target:
        raise ValueError("El objetivo es menor que la suma de los mínimos")

    remaining = target - sum(allocation)
    active = {
        index
        for index in range(size)
        if allocation[index] < capacities[index]
    }

    while remaining > 0:
        if not active:
            raise ValueError("La capacidad total es menor que el tamaño solicitado")
        total_weight = sum(weights[index] for index in active)
        if total_weight == 0:
            for index in sorted(active, key=lambda item: tie_keys[item]):
                allocation[index] += 1
                remaining -= 1
                if remaining == 0:
                    break
            active = {
                index
                for index in active
                if allocation[index] < capacities[index]
            }
            continue

        exact_quotas = {
            index: Fraction(remaining * weights[index], total_weight)
            for index in active
        }
        capped = [
            index
            for index in active
            if exact_quotas[index] >= capacities[index] - allocation[index]
        ]
        if capped:
            for index in capped:
                increment = capacities[index] - allocation[index]
                allocation[index] += increment
                remaining -= increment
            active.difference_update(capped)
            continue

        floors = {
            index: exact_quotas[index].numerator // exact_quotas[index].denominator
            for index in active
        }
        for index, increment in floors.items():
            allocation[index] += increment
        remaining -= sum(floors.values())

        fractional_remainders = {
            index: exact_quotas[index] - floors[index]
            for index in active
        }
        order = sorted(
            active,
            key=lambda index: (-fractional_remainders[index], tie_keys[index]),
        )
        for index in order:
            if remaining == 0:
                break
            if allocation[index] < capacities[index]:
                allocation[index] += 1
                remaining -= 1

        active = {
            index
            for index in active
            if allocation[index] < capacities[index]
        }

    if remaining != 0 or sum(allocation) != target:
        raise AssertionError("La asignación proporcional no alcanzó la cantidad exacta")
    return allocation


def allocate_class_samples(
    files: Sequence[SourceFile], sample_size: int
) -> tuple[dict[str, int], dict[str, int]]:
    class_counts: dict[str, int] = {}
    for source in files:
        class_counts[source.label] = class_counts.get(source.label, 0) + source.row_count

    empty_classes = sorted(label for label, count in class_counts.items() if count == 0)
    if empty_classes:
        raise ValueError(f"Categorías sin registros; no se pueden preservar: {empty_classes}")

    total_rows = sum(class_counts.values())
    if sample_size > total_rows:
        raise ValueError(
            f"Se solicitan {sample_size:,} filas, pero solo existen {total_rows:,}"
        )
    if sample_size < len(class_counts):
        raise ValueError(
            "El tamaño de muestra debe ser al menos la cantidad de clases para "
            "preservarlas todas"
        )

    labels = sorted(class_counts, key=str.casefold)
    capacities = [class_counts[label] for label in labels]
    # Se reserva una fila por clase y el resto se distribuye proporcionalmente
    # sobre las filas restantes de cada clase.
    weights = [max(0, count - 1) for count in capacities]
    minima = [1] * len(labels)
    values = largest_remainder_allocation(
        weights=weights,
        capacities=capacities,
        target=sample_size,
        minimums=minima,
        tie_keys=labels,
    )
    class_quotas = dict(zip(labels, values, strict=True))

    files_by_class: dict[str, list[SourceFile]] = {}
    for source in files:
        files_by_class.setdefault(source.label, []).append(source)

    for label in labels:
        class_files = files_by_class[label]
        file_weights = [source.row_count for source in class_files]
        file_capacities = [source.row_count for source in class_files]
        file_targets = largest_remainder_allocation(
            weights=file_weights,
            capacities=file_capacities,
            target=class_quotas[label],
            minimums=[0] * len(class_files),
            tie_keys=[source.relative_path.casefold() for source in class_files],
        )
        for source, target in zip(class_files, file_targets, strict=True):
            source.sample_count = target

    if sum(source.sample_count for source in files) != sample_size:
        raise AssertionError("La suma de cuotas por archivo no es 600,000")
    return class_counts, class_quotas


def stable_file_seed(master_seed: int, relative_path: str) -> int:
    payload = f"{master_seed}\0{relative_path}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:16], "big")


def choose_row_indices(population: int, sample_size: int, seed: int) -> Sequence[int]:
    """Muestreo uniforme sin reemplazo con memoria O(k), usando el algoritmo de Floyd."""
    if not 0 <= sample_size <= population:
        raise ValueError("Muestra de archivo fuera de rango")
    if sample_size == 0:
        return ()
    if sample_size == population:
        return range(population)

    rng = random.Random(seed)
    selected: set[int] = set()
    for upper_bound in range(population - sample_size, population):
        candidate = rng.randrange(upper_bound + 1)
        if candidate in selected:
            selected.add(upper_bound)
        else:
            selected.add(candidate)
    return sorted(selected)


def source_file_order(files: Sequence[SourceFile], seed: int) -> list[SourceFile]:
    selected_files = [source for source in files if source.sample_count > 0]
    rng = random.Random(seed)
    rng.shuffle(selected_files)
    return selected_files


def write_sample(
    files: Sequence[SourceFile],
    ordered_files: Sequence[SourceFile],
    expected_header: bytes,
    source_columns: Sequence[str],
    output_temp: Path,
    seed: int,
    lines_per_block: int,
    expected_total: int,
) -> tuple[int, dict[str, int]]:
    print("[2/5] Seleccionando y escribiendo la muestra en un archivo temporal...", flush=True)
    output_temp.parent.mkdir(parents=True, exist_ok=True)
    if output_temp.exists():
        output_temp.unlink()

    expected_commas = len(source_columns) - 1
    total_written = 0
    written_by_class: Counter[str] = Counter()
    output_buffer: list[bytes] = []
    output_buffer_bytes = 0
    header_written = False
    started = time.perf_counter()

    with output_temp.open("wb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as destination:
        for file_index, source in enumerate(ordered_files, start=1):
            indices = choose_row_indices(
                population=source.row_count,
                sample_size=source.sample_count,
                seed=stable_file_seed(seed, source.relative_path),
            )
            encoded_label = source.label.encode("utf-8")
            pointer = 0
            row_offset = 0
            selected_for_file = 0

            with source.path.open("rb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as handle:
                raw_header = handle.readline()
                if strip_line_ending(raw_header) != expected_header:
                    raise ValueError(f"La cabecera cambió durante el procesamiento: {source.path}")

                if not header_written:
                    destination.write(expected_header + b"," + LABEL_COLUMN.encode("ascii") + b"\n")
                    header_written = True

                while pointer < len(indices):
                    block = list(islice(handle, lines_per_block))
                    if not block:
                        break
                    block_start = row_offset
                    block_end = row_offset + len(block)
                    row_offset = block_end

                    while pointer < len(indices) and indices[pointer] < block_start:
                        raise AssertionError("Índice de fila seleccionado quedó atrás")

                    while pointer < len(indices) and indices[pointer] < block_end:
                        local_index = indices[pointer] - block_start
                        core = strip_line_ending(block[local_index])
                        if not core:
                            raise ValueError(
                                f"Fila vacía en {source.relative_path}, índice {indices[pointer]}"
                            )
                        if core.count(b",") != expected_commas:
                            raise ValueError(
                                f"Columnas inválidas en {source.relative_path}, índice "
                                f"{indices[pointer]}: se esperaban {len(source_columns)}, "
                                f"se encontraron {core.count(b',') + 1}"
                            )

                        output_line = core + b"," + encoded_label + b"\n"
                        output_buffer.append(output_line)
                        output_buffer_bytes += len(output_line)
                        total_written += 1
                        selected_for_file += 1
                        written_by_class[source.label] += 1
                        pointer += 1

                        if (
                            len(output_buffer) >= DEFAULT_OUTPUT_ROWS_PER_BUFFER
                            or output_buffer_bytes >= DEFAULT_OUTPUT_BUFFER_BYTES
                        ):
                            destination.write(b"".join(output_buffer))
                            output_buffer.clear()
                            output_buffer_bytes = 0

            if indices and indices[-1] >= source.row_count:
                raise AssertionError(
                    f"La última posición seleccionada es inválida en {source.relative_path}"
                )
            if selected_for_file != source.sample_count:
                raise AssertionError(
                    f"Selección incompleta de {source.relative_path}: "
                    f"esperadas {source.sample_count}, seleccionadas {selected_for_file}"
                )

            if file_index == 1 or file_index % 20 == 0 or file_index == len(ordered_files):
                elapsed = max(time.perf_counter() - started, 1e-9)
                print(
                    f"      {file_index:>3}/{len(ordered_files)} archivos con muestra | "
                    f"{total_written:,}/{expected_total:,} filas | "
                    f"{total_written / elapsed:,.0f} filas/s",
                    flush=True,
                )

        if output_buffer:
            destination.write(b"".join(output_buffer))
        destination.flush()
        os.fsync(destination.fileno())

    if not header_written:
        raise ValueError("No se escribió ninguna fila de datos")
    return total_written, dict(written_by_class)


def validate_sample(
    output_temp: Path,
    expected_header: bytes,
    source_columns: Sequence[str],
    expected_class_counts: dict[str, int],
    expected_total: int,
    sqlite_temp: Path,
) -> dict[str, object]:
    print("[3/5] Validando filas, columnas, nulos, duplicados y clases...", flush=True)
    if sqlite_temp.exists():
        sqlite_temp.unlink()

    expected_columns = [*source_columns, LABEL_COLUMN]
    expected_column_count = len(expected_columns)
    null_by_index: Counter[int] = Counter()
    observed_class_counts: Counter[str] = Counter()
    rows_with_null = 0
    row_count = 0
    connection: sqlite3.Connection | None = None

    try:
        connection = sqlite3.connect(sqlite_temp)
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        connection.execute("PRAGMA cache_size=-8192")
        connection.execute(
            "CREATE TABLE seen_rows (row_data BLOB PRIMARY KEY) WITHOUT ROWID"
        )
        cursor = connection.cursor()

        with output_temp.open("rb", buffering=DEFAULT_COUNT_BUFFER_BYTES) as handle:
            raw_header = handle.readline()
            if strip_line_ending(raw_header) != expected_header + b"," + LABEL_COLUMN.encode():
                raise ValueError("La cabecera del archivo temporal no coincide con la esperada")

            for line_number, line in enumerate(handle, start=2):
                core = strip_line_ending(line)
                if not core:
                    raise ValueError(f"Fila vacía en el archivo temporal, línea {line_number}")

                fields = core.split(b",")
                if len(fields) != expected_column_count:
                    raise ValueError(
                        f"Columnas inválidas en la línea {line_number}: "
                        f"esperadas {expected_column_count}, encontradas {len(fields)}"
                    )

                row_has_null = False
                for column_index, value in enumerate(fields):
                    if value.strip() in NULL_MARKERS:
                        null_by_index[column_index] += 1
                        row_has_null = True
                if row_has_null:
                    rows_with_null += 1

                try:
                    label = fields[-1].decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ValueError(
                        f"Etiqueta inválida en la línea {line_number}"
                    ) from exc
                observed_class_counts[label] += 1
                cursor.execute(
                    "INSERT OR IGNORE INTO seen_rows(row_data) VALUES (?)",
                    (sqlite3.Binary(core),),
                )
                row_count += 1

                if row_count % 50_000 == 0:
                    connection.commit()

        connection.commit()
        unique_rows = int(connection.execute("SELECT COUNT(*) FROM seen_rows").fetchone()[0])
        duplicate_rows = row_count - unique_rows

        if row_count != expected_total:
            raise ValueError(
                f"Cantidad final inválida: esperadas {expected_total:,}, encontradas {row_count:,}"
            )
        if observed_class_counts != Counter(expected_class_counts):
            missing = sorted(set(expected_class_counts) - set(observed_class_counts))
            extra = sorted(set(observed_class_counts) - set(expected_class_counts))
            differences = {
                label: (expected_class_counts[label], observed_class_counts[label])
                for label in sorted(set(expected_class_counts) & set(observed_class_counts))
                if expected_class_counts[label] != observed_class_counts[label]
            }
            raise ValueError(
                "La distribución de clases no coincide con la cuota. "
                f"Faltantes={missing}, extra={extra}, diferencias={differences}"
            )

        null_by_column = {
            column: int(null_by_index.get(index, 0))
            for index, column in enumerate(expected_columns)
        }
        return {
            "row_count": row_count,
            "column_count": expected_column_count,
            "columns": expected_columns,
            "class_counts": dict(sorted(observed_class_counts.items(), key=lambda item: item[0].casefold())),
            "duplicate_rows": duplicate_rows,
            "unique_rows": unique_rows,
            "null_cells": sum(null_by_index.values()),
            "null_by_column": null_by_column,
            "rows_with_at_least_one_null": rows_with_null,
        }
    finally:
        if connection is not None:
            connection.close()
        for path in (sqlite_temp, Path(f"{sqlite_temp}-journal"), Path(f"{sqlite_temp}-wal")):
            try:
                path.unlink()
            except FileNotFoundError:
                pass


def approximate_pandas_memory_bytes(
    row_count: int,
    feature_columns: int,
    class_counts: dict[str, int],
) -> int:
    # Estimación para un DataFrame de una sola pieza: 8 bytes por columna numérica,
    # índice, punteros de objetos y una cadena Python por etiqueta.
    if row_count == 0:
        return 0
    average_label_utf8_bytes = (
        sum(len(label.encode("utf-8")) * count for label, count in class_counts.items())
        / row_count
    )
    bytes_per_row = (
        feature_columns * 8
        + 8  # RangeIndex
        + 8  # punteros del ndarray de objetos
        + sys.getsizeof("")
        + average_label_utf8_bytes
    )
    return int(round(row_count * bytes_per_row))


def build_report(
    files: Sequence[SourceFile],
    source_columns: Sequence[str],
    source_class_counts: dict[str, int],
    class_quotas: dict[str, int],
    validation: dict[str, object],
    output_temp: Path,
    report_path: Path,
    sample_size: int,
    seed: int,
    lines_per_block: int,
    durations: dict[str, float],
) -> dict[str, object]:
    output_size = output_temp.stat().st_size
    final_class_counts = validation["class_counts"]
    if not isinstance(final_class_counts, dict):
        raise TypeError("Conteos finales de clase inválidos")

    distribution: list[dict[str, object]] = []
    for label in sorted(source_class_counts, key=str.casefold):
        original_count = source_class_counts[label]
        sampled_count = int(final_class_counts[label])
        original_percentage = original_count / sum(source_class_counts.values()) * 100
        sampled_percentage = sampled_count / sample_size * 100
        distribution.append(
            {
                "label": label,
                "original_count": original_count,
                "original_percentage": round(original_percentage, 6),
                "sample_count": sampled_count,
                "sample_percentage": round(sampled_percentage, 6),
                "difference_percentage_points": round(
                    sampled_percentage - original_percentage, 6
                ),
            }
        )

    source_size = sum(source.path.stat().st_size for source in files)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "root_name": "CIC-IoT-2023",
            "csv_files": len(files),
            "classes": len(source_class_counts),
            "source_size_bytes": source_size,
            "source_size_gib": round(source_size / (1024**3), 6),
            "total_rows": sum(source_class_counts.values()),
            "label_derivation": (
                "Parent directory; Benign_Final is normalized to Benign. "
                "All other directory names are preserved."
            ),
        },
        "sample": {
            "output_file": output_path_name(report_path, output_temp),
            "rows": validation["row_count"],
            "data_rows_excluding_header": validation["row_count"],
            "columns": validation["column_count"],
            "column_names": validation["columns"],
            "file_size_bytes": output_size,
            "file_size_mib": round(output_size / (1024**2), 3),
            "file_size_gib": round(output_size / (1024**3), 6),
            "estimated_pandas_memory_bytes": approximate_pandas_memory_bytes(
                int(validation["row_count"]),
                len(source_columns),
                {str(key): int(value) for key, value in final_class_counts.items()},
            ),
            "original_feature_values_preserved": True,
            "header_included": True,
        },
        "quality": {
            "duplicate_rows_full_record": validation["duplicate_rows"],
            "unique_rows": validation["unique_rows"],
            "null_cells": validation["null_cells"],
            "null_by_column": validation["null_by_column"],
            "rows_with_at_least_one_null": validation["rows_with_at_least_one_null"],
        },
        "class_distribution": distribution,
        "sampling": {
            "method": (
                "Two-pass streaming sample. One row is reserved for every class; "
                "the remaining rows are allocated proportionally with capped "
                "largest-remainder rounding. Quotas are then allocated among files "
                "and exact random row positions are selected without replacement."
            ),
            "random_state": seed,
            "minimum_rows_per_class": 1,
            "all_classes_preserved": True,
            "total_rows_exact": validation["row_count"] == sample_size,
            "lines_per_block": lines_per_block,
        },
        "performance": {
            "count_and_inventory_seconds": round(durations["inventory"], 3),
            "sampling_and_write_seconds": round(durations["sampling"], 3),
            "validation_seconds": round(durations["validation"], 3),
            "full_dataset_loaded_in_memory": False,
        },
    }


def output_path_name(report_path: Path, output_temp: Path) -> str:
    # El nombre final se deriva del nombre temporal sin el sufijo .partial.
    if output_temp.name.endswith(".partial"):
        return output_temp.name[: -len(".partial")]
    return str(output_temp)


def write_json_atomic(data: dict[str, object], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_temp = report_path.with_suffix(report_path.suffix + ".partial")
    if report_temp.exists():
        report_temp.unlink()
    with report_temp.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(report_temp, report_path)


def main() -> None:
    args = parse_args()
    if args.sample_size <= 0:
        raise ValueError("--sample-size debe ser positivo")
    if args.lines_per_block <= 0:
        raise ValueError("--lines-per-block debe ser positivo")

    input_dir = args.input_dir.resolve()
    output_path = args.output.resolve()
    report_path = args.report.resolve()
    if output_path == report_path:
        raise ValueError("El CSV y el reporte no pueden usar la misma ruta")
    if not input_dir.is_dir():
        raise NotADirectoryError(input_dir)

    print("=== Preparation of CIC-IoT-2023 sample ===", flush=True)
    print(f"Input:   {input_dir}", flush=True)
    print(f"Output:  {output_path}", flush=True)
    print(f"Rows:    {args.sample_size:,}", flush=True)
    print(f"Seed:    {args.seed}", flush=True)

    overall_started = time.perf_counter()
    files = discover_source_files(input_dir, output_path)
    print(
        f"Detectados {len(files):,} CSV en {len({source.label for source in files})} clases.",
        flush=True,
    )

    inventory_started = time.perf_counter()
    expected_header, source_columns = scan_sources(files)
    source_class_counts, class_quotas = allocate_class_samples(files, args.sample_size)
    inventory_duration = time.perf_counter() - inventory_started

    print("[1/5] Conteos completados.", flush=True)
    for label in sorted(source_class_counts, key=str.casefold):
        print(
            f"      {label:<24} original={source_class_counts[label]:>12,}  "
            f"muestra={class_quotas[label]:>9,}",
            flush=True,
        )

    output_temp = output_path.with_suffix(output_path.suffix + ".partial")
    sqlite_temp = output_path.with_name(f".{output_path.name}_duplicates.sqlite.tmp")
    ordered_files = source_file_order(files, args.seed)

    sampling_started = time.perf_counter()
    rows_written, written_by_class = write_sample(
        files=files,
        ordered_files=ordered_files,
        expected_header=expected_header,
        source_columns=source_columns,
        output_temp=output_temp,
        seed=args.seed,
        lines_per_block=args.lines_per_block,
        expected_total=args.sample_size,
    )
    sampling_duration = time.perf_counter() - sampling_started

    if rows_written != args.sample_size:
        raise AssertionError("El escritor no generó la cantidad exacta de filas")
    if written_by_class != Counter(class_quotas):
        raise AssertionError("El escritor no respetó las cuotas por clase")

    validation_started = time.perf_counter()
    validation = validate_sample(
        output_temp=output_temp,
        expected_header=expected_header,
        source_columns=source_columns,
        expected_class_counts=class_quotas,
        expected_total=args.sample_size,
        sqlite_temp=sqlite_temp,
    )
    validation_duration = time.perf_counter() - validation_started
    print(
        f"      Validación: {validation['row_count']:,} filas, "
        f"{validation['column_count']} columnas, "
        f"{validation['duplicate_rows']:,} duplicados, "
        f"{validation['null_cells']:,} celdas nulas.",
        flush=True,
    )

    print("[4/5] Publicando el CSV validado...", flush=True)
    os.replace(output_temp, output_path)

    durations = {
        "inventory": inventory_duration,
        "sampling": sampling_duration,
        "validation": validation_duration,
    }
    report = build_report(
        files=files,
        source_columns=source_columns,
        source_class_counts=source_class_counts,
        class_quotas=class_quotas,
        validation=validation,
        output_temp=Path(str(output_path)),
        report_path=report_path,
        sample_size=args.sample_size,
        seed=args.seed,
        lines_per_block=args.lines_per_block,
        durations=durations,
    )
    write_json_atomic(report, report_path)

    total_duration = time.perf_counter() - overall_started
    print("[5/5] Completado.", flush=True)
    print(f"CSV:    {output_path}", flush=True)
    print(f"Reporte:{report_path}", flush=True)
    print(f"Tiempo total: {total_duration / 60:.2f} minutos", flush=True)


if __name__ == "__main__":
    main()
