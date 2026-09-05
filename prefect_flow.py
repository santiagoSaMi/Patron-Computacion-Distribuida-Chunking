#!/usr/bin/env python3
"""
prefect_flow.py
Orquesta el ciclo de vida completo del laboratorio con Prefect:
  1. Validar el clúster Dask y detectar cuántos workers/máquinas hay
  2. Verificar/generar los datos crudos
  3. Dividir el pipeline de limpieza en tantos grupos como workers detectados
     y ejecutarlos con .map() -> ejecutar_pipeline_dask-0, -1, -2, ...
     Cada ejecución de tarea publica un artifact con el detalle de su worker
  4. Validar quality gates sobre los resultados de todos los grupos
"""
import json
import os
import subprocess
import sys

from prefect import flow, task, get_run_logger, unmapped
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from dask.distributed import Client

RAW_DIR = "shared-data/raw"
OUT_DIR = "shared-data/processed"
SCHEDULER_ADDRESS = os.environ.get("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")


@task(retries=3, retry_delay_seconds=10, name="validar_infraestructura")
def validar_infraestructura():
    logger = get_run_logger()
    logger.info(f"Verificando conexión al Scheduler en {SCHEDULER_ADDRESS}")
    client = Client(SCHEDULER_ADDRESS, timeout="15s")
    n_workers = len(client.scheduler_info()["workers"])
    logger.info(f"Scheduler activo. Workers/máquinas conectados: {n_workers}")
    client.close()
    if n_workers < 1:
        raise RuntimeError("No se encontró ningún worker Dask conectado")
    return n_workers


@task(name="verificar_datos_crudos")
def verificar_datos_crudos():
    logger = get_run_logger()
    if not os.path.isdir(RAW_DIR) or not any(
        f.startswith("transactions_dirty_part_") for f in os.listdir(RAW_DIR)
    ):
        logger.info("No se encontraron datos crudos. Generando dataset sintético...")
        subprocess.run([sys.executable, "generate_dirty_data.py"], check=True)
    else:
        logger.info("Datos crudos ya existen en shared-data/raw/")
    return True


@task(retries=2, retry_delay_seconds=15, name="ejecutar_pipeline_dask")
def ejecutar_pipeline_dask(group_id: int, n_workers: int):
    logger = get_run_logger()
    logger.info(
        f"Grupo {group_id}/{n_workers} -> lanzando su porción del pipeline en el clúster Dask..."
    )
    subprocess.run(
        [
            sys.executable, "dask_pipeline.py",
            "--group-id", str(group_id),
            "--num-groups", str(n_workers),
        ],
        check=True,
    )

    stats_path = os.path.join(OUT_DIR, f"worker_stats_group_{group_id}.json")
    with open(stats_path, "r", encoding="utf-8") as f:
        stats = json.load(f)

    logger.info(f"Grupo {group_id} — filas procesadas: {stats['total_rows']:,}")
    for w in stats["workers"]:
        logger.info(
            f"  Worker {w['worker']} ({w['address']}) — "
            f"{w['nthreads']} threads, {w['metrics_memory_gb']} GB en uso"
        )

    create_table_artifact(
        key=f"dask-worker-detalle-grupo-{group_id}",
        table=stats["workers"],
        description=(
            f"Grupo {group_id}: {stats['total_rows']:,} filas, "
            f"{stats['anomalies']:,} anomalías ({stats['anomaly_rate']:.2%})."
        ),
    )

    return stats


@task(name="resumen_global")
def resumen_global(resultados: list):
    total_rows = sum(r["total_rows"] for r in resultados)
    total_anomalies = sum(r["anomalies"] for r in resultados)
    anomaly_rate = total_anomalies / total_rows if total_rows else 0

    resumen_md = (
        f"## Resumen global del pipeline distribuido\n\n"
        f"- **Grupos ejecutados:** {len(resultados)}\n"
        f"- **Filas procesadas (total):** {total_rows:,}\n"
        f"- **Anomalías detectadas (total):** {total_anomalies:,} "
        f"({anomaly_rate:.2%})\n"
    )
    create_markdown_artifact(key="resumen-pipeline-global", markdown=resumen_md)
    return {"total_rows": total_rows, "anomaly_rate": anomaly_rate}


@task(name="quality_gate_final")
def quality_gate_final(n_workers: int):
    logger = get_run_logger()
    faltantes = []
    for group_id in range(n_workers):
        out_path = os.path.join(OUT_DIR, f"transactions_clean_group_{group_id}.parquet")
        if not os.path.exists(out_path):
            faltantes.append(out_path)
        else:
            logger.info(f"Resultado del grupo {group_id} verificado en: {out_path}")
    if faltantes:
        raise RuntimeError(f"Faltan resultados esperados: {faltantes}")
    return True


@flow(name="taller-dask-prefect-cluster")
def taller_flow():
    n_workers = validar_infraestructura()
    verificar_datos_crudos()

    # Un grupo por cada worker/máquina detectada -> ejecutar_pipeline_dask-0,
    # ejecutar_pipeline_dask-1, ... hasta n_workers-1, visibles como
    # ejecuciones separadas en la UI de Prefect
    resultados = ejecutar_pipeline_dask.map(
        group_id=list(range(n_workers)),
        n_workers=unmapped(n_workers),
    )

    global_stats = resumen_global(resultados)
    quality_gate_final(n_workers)

    return {
        "workers_activos": n_workers,
        "anomaly_rate": global_stats["anomaly_rate"],
        "status": "COMPLETED",
    }


if __name__ == "__main__":
    resultado = taller_flow()
    print(resultado)
