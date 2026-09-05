# Taller: Computación Distribuida con Dask, Docker & Prefect

## Orden de ejecución

```bash
# 1. Construir e iniciar el clúster (scheduler + 3 workers + cliente)
docker compose up -d --build

# 2. Verificar que los 4 contenedores estén corriendo
docker compose ps

# 3. Abrir el Dask Dashboard en el navegador
#    http://localhost:8787

# 4. Entrar al contenedor cliente
docker compose exec client bash

# --- Dentro del contenedor client ---

# 5. Generar los 300.000 registros sucios (6 particiones CSV)
python3 generate_dirty_data.py

# 6. Ejecutar el pipeline de limpieza distribuido directamente (opcional, para probarlo suelto)
python3 dask_pipeline.py

# 7. Ejecutar todo el ciclo de vida orquestado con Prefect
python3 prefect_flow.py
```

## Verificar tolerancia a fallos (pregunta 2 de la guía)

En otra terminal, mientras `dask_pipeline.py` está corriendo:

```bash
docker stop dask-worker-2
```

Observa en el Dashboard (http://localhost:8787/status) cómo el Scheduler
reasigna las tareas pendientes de `dask-worker-2` a `dask-worker-1` y
`dask-worker-3` usando el grafo de dependencias (DAG) que ya tenía registrado.

## Estructura de archivos

```
.
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── generate_dirty_data.py   # Paso 3 de la guía: generador de datos sucios
├── dask_pipeline.py         # Paso 4: limpieza distribuida (mojibake, regex, teléfonos)
├── prefect_flow.py          # Paso 5: orquestación con @flow / @task
└── shared-data/
    ├── raw/                 # CSVs generados
    └── processed/           # Parquet final limpio
```

## Notas sobre `deploy.resources.limits`

Docker Compose v2 (sin Swarm) ignora `deploy.resources.limits` a menos que
ejecutes con `docker compose --compatibility up`. Si tu profesor pide ver los
límites de CPU/RAM realmente aplicados, usa:

```bash
docker compose --compatibility up -d --build
```
