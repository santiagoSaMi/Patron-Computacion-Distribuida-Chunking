# Guía de Laboratorio Avanzado: Computación Distribuida con Dask, Docker & Prefect

**Curso:** Arquitectura de Software Avanzada — Módulo de Patrones Arquitectónicos Avanzados / Sistemas Distribuidos y Big Data
**Autor:** Santiago Sabogal Millan
**Institución:** Universidad de La Sabana

---

## 1. Descripción general

Este proyecto implementa el patrón arquitectónico **Master-Worker** para el procesamiento *out-of-core* de un conjunto sintético de 300.000 registros con anomalías severas de calidad de datos (mojibake de codificación, identificadores heterogéneos y valores nulos). La solución despliega un clúster distribuido de cómputo con **Dask** sobre contenedores **Docker**, y orquesta el ciclo de vida completo del pipeline con **Prefect**, incluyendo observabilidad y *quality gates* automatizados.

## 2. Objetivos de aprendizaje cubiertos

| # | Objetivo | Evidencia en el proyecto |
|---|----------|---------------------------|
| 1 | Despliegue de infraestructura distribuida | `docker-compose.yml`: 1 Scheduler + N Workers en red Docker aislada |
| 2 | Computación out-of-core y particionado | `dask.dataframe` sobre 6 particiones CSV, cómputo perezoso |
| 3 | Resiliencia y limpieza de calidad de datos | `dask_pipeline.py`: reparación de mojibake y extracción por regex |
| 4 | Refactorización de columnas en clúster | Estandarización de `raw_customer_code` → `CUST-XXXXX` |
| 5 | Orquestación y observabilidad | `prefect_flow.py`: `@flow`, `@task`, reintentos, artifacts y Dask Dashboard |

## 3. Arquitectura del clúster

```
                         ┌───────────────────────┐
                         │   dask-scheduler       │
                         │   (Master / Coordinador)│
                         │   Puertos: 8786, 8787  │
                         └───────────┬───────────┘
                                     │  red: dask-cluster-net
             ┌───────────────────────┼───────────────────────┐
             │                       │                       │
   ┌─────────▼────────┐   ┌─────────▼────────┐   ┌─────────▼────────┐
   │  dask-worker-1    │   │  dask-worker-2    │   │  dask-worker-3    │
   │  2 threads/1.5 GB │   │  2 threads/1.5 GB │   │  2 threads/1.5 GB │
   └────────────────────┘   └────────────────────┘   └────────────────────┘

   ┌───────────────────────────────────────────────────────────────┐
   │  client (orquestador)                                          │
   │  - generate_dirty_data.py                                      │
   │  - dask_pipeline.py                                            │
   │  - prefect_flow.py  →  Prefect Server UI (puerto 4200)          │
   └───────────────────────────────────────────────────────────────┘
```

El número de grupos de trabajo se calcula dinámicamente a partir de los workers detectados en tiempo de ejecución (`n_workers`), de modo que el pipeline escala automáticamente si se agregan o retiran nodos del clúster.

## 4. Estructura del repositorio

```
.
├── docker-compose.yml       # Topología del clúster (1 scheduler, N workers, 1 cliente)
├── Dockerfile                # Imagen base común para todos los servicios
├── requirements.txt          # Dependencias fijadas (Dask, Prefect, pydantic, griffe, etc.)
├── generate_dirty_data.py    # Generador de 300.000 registros sintéticos con ruido
├── dask_pipeline.py          # Pipeline distribuido de limpieza y refactorización
├── prefect_flow.py           # Orquestación del ciclo de vida completo con Prefect
└── shared-data/
    ├── raw/                  # CSV crudos generados (6 particiones)
    └── processed/            # Parquet limpio por grupo + estadísticas por worker (JSON)
```

## 5. Instrucciones de ejecución

```bash
# 1. Construir y levantar el clúster completo
docker compose up -d --build

# 2. Verificar que los servicios estén activos
docker compose ps

# 3. Ingresar al contenedor cliente
docker compose exec client bash

# 4. (Dentro del contenedor) Levantar el servidor de Prefect
prefect server start --host 0.0.0.0

# 5. En una segunda terminal, ingresar de nuevo al contenedor y ejecutar el flujo
docker compose exec client bash
python3 prefect_flow.py
```

**Interfaces disponibles:**

| Servicio | URL |
|----------|-----|
| Dask Dashboard | http://localhost:8787 |
| Prefect Server UI | http://localhost:4200 |

## 6. Estrategia de limpieza de datos

| Columna origen | Transformación | Columna resultante |
|----------------|-----------------|---------------------|
| `raw_customer_code` | Extracción regex `(?:CLI-\|cli_\|RAW#\|CUST-)?(\d{5})`; token `CUST-00000-ANOMALY` si no hay coincidencia | `customer_code` |
| `city_notes_corrupted` | Corrección de mojibake (`encode("latin-1").decode("utf-8")`) | `city_notes` |
| `phone_raw` | Extracción de dígitos y normalización a formato `+57XXXXXXXXXX` | `phone_e164` |

El resultado se persiste en formato **Apache Parquet**, con compresión columnar y soporte de *predicate pushdown*, en lugar de CSV.

## 7. Observabilidad y control de calidad

- Cada grupo de trabajo publica un **artifact de tabla** en Prefect (`dask-worker-detalle-grupo-N`) con la dirección, threads y memoria del worker que lo procesó.
- Una tarea `resumen_global` consolida las estadísticas de todos los grupos en un artifact de resumen.
- `quality_gate_final` valida que exista el archivo Parquet esperado por cada grupo antes de marcar el flujo como exitoso; el pipeline también aborta si la tasa de anomalías en `customer_code` supera el 10&nbsp;%.

## 8. Prueba de tolerancia a fallos

Para verificar la recomputación ante la caída de un nodo (pregunta 2 de la guía), con el flujo en ejecución:

```bash
docker stop dask-worker-2
```

El Dask Dashboard (`http://localhost:8787/status`) permite observar cómo el Scheduler reasigna las tareas pendientes de `dask-worker-2` a los workers restantes utilizando el grafo de dependencias (DAG) ya registrado, sin pérdida de resultados parciales ya confirmados.

## 9. Notas técnicas de compatibilidad

`requirements.txt` fija explícitamente `griffe==0.38.1` y `pydantic==2.9.2` para evitar incompatibilidades internas conocidas con `prefect==2.19.5`. Se recomienda reconstruir la imagen (`docker compose up -d --build`) en lugar de instalar paquetes sueltos dentro del contenedor, para preservar la consistencia de versiones.

`deploy.resources.limits` en `docker-compose.yml` solo se aplica en modo Swarm; para que los límites de CPU/RAM se respeten en Compose estándar, ejecutar:

```bash
docker compose --compatibility up -d --build
```

## 10. Preguntas de análisis arquitectónico

Este repositorio provee la infraestructura y evidencia necesarias para responder, en el informe escrito entregable, las cuatro preguntas de la guía: (1) cuellos de botella de red vs. CPU, (2) tolerancia a fallos y recomputación, (3) ventajas de Parquet sobre CSV, y (4) separación de responsabilidades entre Prefect y Dask.
