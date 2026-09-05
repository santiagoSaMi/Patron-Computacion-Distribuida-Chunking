#!/usr/bin/env python3
"""
dask_pipeline.py
Pipeline Out-of-Core sobre el clúster Dask (1 scheduler + 3 workers).

Transformaciones:
  1. raw_customer_code -> customer_code canónico 'CUST-XXXXX'
  2. city_notes_corrupted -> city_notes (mojibake reparado)
  3. phone_raw -> phone_e164 (formato normalizado)
Salida: shared-data/processed/transactions_clean.parquet (particionado)
"""
import argparse
import json
import os
import re
import sys

import pandas as pd
from dask.distributed import Client

RAW_DIR = "shared-data/raw"
OUT_DIR = "shared-data/processed"
SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")

# Patrón: extrae el número de 5 dígitos ignorando cualquier prefijo conocido
CODE_PATTERN = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})")
ANOMALY_TOKEN = "CUST-00000-ANOMALY"


# ---------------------------------------------------------------------------
# 1. Corrección de mojibake (UTF-8 mal interpretado como Latin-1 / cp1252)
# ---------------------------------------------------------------------------
def fix_mojibake(text) -> str:
    if pd.isna(text) or not isinstance(text, str) or text.strip() == "":
        return ""
    try:
        # El texto fue codificado en UTF-8 y luego decodificado erróneamente
        # como Latin-1/cp1252. Revertimos el proceso: re-encode a Latin-1
        # y decode como UTF-8.
        repaired = text.encode("latin-1").decode("utf-8")
        return repaired
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Si no es mojibake reparable con este método, se devuelve tal cual
        return text


# ---------------------------------------------------------------------------
# 2. Refactorización del código de cliente
# ---------------------------------------------------------------------------
def normalize_customer_code(raw) -> str:
    if pd.isna(raw) or not isinstance(raw, str):
        return ANOMALY_TOKEN
    match = CODE_PATTERN.search(raw.strip())
    if not match:
        return ANOMALY_TOKEN
    return f"CUST-{match.group(1)}"


# ---------------------------------------------------------------------------
# 3. Normalización de teléfonos a formato E.164 aproximado (+57XXXXXXXXXX)
# ---------------------------------------------------------------------------
def normalize_phone(raw) -> str:
    if pd.isna(raw) or not isinstance(raw, str):
        return "PHONE-UNKNOWN"
    digits = re.sub(r"\D", "", raw)
    # Quitar prefijo de país si viene incluido (57)
    if digits.startswith("57") and len(digits) > 10:
        digits = digits[2:]
    if len(digits) != 10:
        return "PHONE-UNKNOWN"
    return f"+57{digits}"


# ---------------------------------------------------------------------------
# Función que se ejecuta POR PARTICIÓN en cada worker (map_partitions)
# ---------------------------------------------------------------------------
def clean_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.copy()
    pdf["customer_code"] = pdf["raw_customer_code"].apply(normalize_customer_code)
    pdf["city_notes"] = pdf["city_notes_corrupted"].apply(fix_mojibake)
    pdf["phone_e164"] = pdf["phone_raw"].apply(normalize_phone)
    pdf["is_anomaly"] = pdf["customer_code"].eq(ANOMALY_TOKEN)
    return pdf[
        [
            "transaction_id",
            "customer_code",
            "city_notes",
            "phone_e164",
            "amount_usd",
            "business_category",
            "is_anomaly",
        ]
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--group-id",
        type=int,
        default=None,
        help="Opcional: índice de grupo (0..num_groups-1) para procesar solo el "
             "subconjunto de CSV que le corresponde a esa máquina/worker. Si se "
             "omite, procesa TODOS los archivos.",
    )
    parser.add_argument(
        "--num-groups",
        type=int,
        default=3,
        help="En cuántos grupos dividir los 6 CSV (normalmente = cantidad de "
             "workers/máquinas detectadas en el clúster).",
    )
    parser.add_argument(
        "--worker-stats-out",
        default=None,
        help="Ruta donde guardar el JSON con estadísticas del worker de este "
             "grupo. Por defecto: worker_stats.json (sin grupo) o "
             "worker_stats_group_{N}.json (con --group-id).",
    )
    args = parser.parse_args()

    if args.worker_stats_out is None:
        suffix = f"_group_{args.group_id}" if args.group_id is not None else ""
        args.worker_stats_out = os.path.join(OUT_DIR, f"worker_stats{suffix}.json")

    print(f"[*] Conectando al Scheduler en {SCHEDULER_ADDRESS} ...")
    client = Client(SCHEDULER_ADDRESS)
    print(client)
    print(f"[*] Dashboard disponible en: {client.dashboard_link}")

    import dask.dataframe as dd

    ALL_FILES = sorted(
        f for f in os.listdir(RAW_DIR) if f.startswith("transactions_dirty_part_")
    )

    if args.group_id is None:
        input_glob = os.path.join(RAW_DIR, "transactions_dirty_part_*.csv")
        out_name = "transactions_clean.parquet"
        print(f"[*] Leyendo TODAS las particiones desde: {input_glob}")
    else:
        # Reparte los N archivos CSV entre num_groups grupos lo más parejo posible
        n_files = len(ALL_FILES)
        n_groups = max(1, args.num_groups)
        files_per_group = [
            ALL_FILES[i::n_groups] for i in range(n_groups)
        ]  # reparto round-robin, tolera N no divisible entre num_groups
        my_files = files_per_group[args.group_id] if args.group_id < n_groups else []
        files = [os.path.join(RAW_DIR, f) for f in my_files]
        if not files:
            print(f"[*] Grupo {args.group_id}: no le tocó ningún archivo, nada que hacer.")
            client.close()
            return
        input_glob = files
        out_name = f"transactions_clean_group_{args.group_id}.parquet"
        print(f"[*] Grupo {args.group_id}/{n_groups} -> procesando: {files}")

    ddf = dd.read_csv(
        input_glob,
        dtype={
            "raw_customer_code": "object",
            "city_notes_corrupted": "object",
            "phone_raw": "object",
        },
        encoding="utf-8",
    )
    print(f"[*] Particiones cargadas (lazy): {ddf.npartitions}")

    # map_partitions ejecuta clean_partition (lógica Pandas por bloque) en cada worker
    meta = {
        "transaction_id": "object",
        "customer_code": "object",
        "city_notes": "object",
        "phone_e164": "object",
        "amount_usd": "float64",
        "business_category": "object",
        "is_anomaly": "bool",
    }
    cleaned = ddf.map_partitions(clean_partition, meta=meta)

    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, out_name)
    print(f"[*] Ejecutando el grafo de tareas y escribiendo Parquet en: {out_path}")

    # Aquí se dispara realmente el cómputo distribuido (antes todo era perezoso)
    cleaned.to_parquet(out_path, engine="pyarrow", write_index=False, overwrite=True)

    # ------------------------------------------------------------------
    # Quality gate: validar tasa de anomalías tras el procesamiento
    # ------------------------------------------------------------------
    total = cleaned.shape[0].compute()
    anomalies = cleaned["is_anomaly"].sum().compute()
    anomaly_rate = anomalies / total

    print(f"[OK] Filas procesadas: {total:,}")
    print(f"[OK] Anomalías detectadas en customer_code: {anomalies:,} ({anomaly_rate:.2%})")

    # ------------------------------------------------------------------
    # Recolectar estadísticas de CADA worker del clúster para publicarlas
    # después como artifact en Prefect
    # ------------------------------------------------------------------
    info = client.scheduler_info()
    worker_stats = []
    for addr, w in info["workers"].items():
        worker_stats.append(
            {
                "worker": w.get("name", addr),
                "address": addr,
                "nthreads": w.get("nthreads"),
                "memory_limit_gb": round(w.get("memory_limit", 0) / (1024 ** 3), 2),
                "metrics_memory_gb": round(
                    w.get("metrics", {}).get("memory", 0) / (1024 ** 3), 3
                ),
                "executing": w.get("metrics", {}).get("executing", 0),
                "tasks_in_memory": len(w.get("has_what", []))
                if "has_what" in w
                else None,
            }
        )

    os.makedirs(os.path.dirname(args.worker_stats_out) or ".", exist_ok=True)
    with open(args.worker_stats_out, "w", encoding="utf-8") as f:
        json.dump(
            {
                "total_rows": int(total),
                "anomalies": int(anomalies),
                "anomaly_rate": float(anomaly_rate),
                "workers": worker_stats,
            },
            f,
            indent=2,
        )
    print(f"[OK] Estadísticas de workers guardadas en: {args.worker_stats_out}")

    client.close()

    if anomaly_rate > 0.10:
        print("[FALLO] Quality gate: tasa de anomalías > 10%", file=sys.stderr)
        sys.exit(1)

    print("[OK] Quality gate superado.")


if __name__ == "__main__":
    main()
