# Trabajo en equipo: ramas, responsables y pull requests

La guía evalúa que el historial refleje el aporte de cada integrante (ramas y PR) y que **cada uno domine cualquier parte** en la sustentación. Reglas:

1. Nadie hace commit directo a `main`. Cada funcionalidad va en su rama `feature/...` y entra por pull request.
2. **Cada PR lo revisa y aprueba el otro integrante** (comentario + *Approve*). Revisar el PR ajeno es la forma de estudiar esa parte para la sustentación.
3. Cada quien hace commits desde su propia cuenta y su propio equipo, con mensajes que digan qué hizo.
4. Después de cada merge, Jenkins despliega solo (pollSCM). Verifiquen `/health`: el `commit` debe coincidir con el último de `main`.

## Configuración de cada integrante (una vez)

```bash
git config --global user.name  "Su Nombre"
git config --global user.email "correo-de-su-cuenta-github"
git clone https://github.com/Julianlc324/lifemap.git && cd lifemap
```
Julián agrega a Andrés como colaborador: *Settings → Collaborators → Add people*.
Recomendado: *Settings → Branches → Add rule* en `main` con "Require a pull request before merging" y 1 aprobación.

## Reparto

`main` arranca con el esqueleto (Compose, Dockerfiles de Spark y Dask, configuración, README, docs y los `__init__.py` de cada paquete). Hasta que se integren todas las ramas de la Entrega 1 el sistema no arranca completo; es normal que los primeros builds de Jenkins fallen. El resto del código está en la carpeta del proyecto entregada; cada responsable copia sus archivos a su rama, los lee, los prueba y los sube.

| # | Rama | Responsable | Archivos | Revisa |
|---|---|---|---|---|
| 1 | `feature/nucleo-y-formulas` | Julián | `lifemap/metrics.py`, `lifemap/geo.py`, `lifemap/features.py`, `lifemap/steps.py`, `tests/unit/test_metrics.py`, `tests/unit/test_geo_features.py` | Andrés |
| 2 | `feature/ingesta-dask` | Julián | `ingest/ingest.py` | Andrés |
| 3 | `feature/spark-base-y-vida` | Andrés | `lifemap/spark.py`, `lifemap/models.py`, `jobs/run_step.py`, `jobs/business_life.py`, `bin/` | Julián |
| 4 | `feature/sentimiento` | Andrés | `jobs/sentiment_train.py` | Julián |
| 5 | `feature/mapreduce-tasas` | Andrés | `jobs/closure_rates.py`, `jobs/report.py` | Julián |
| 6 | `feature/balanceo` | Andrés | `jobs/load_balancing.py` | Julián |
| 7 | `feature/api` | Julián | `api/app.py`, `api/health.py`, `api/predictor.py`, `api/validation.py`, `api/static/`, `tests/unit/test_api_validation.py`, `tests/integration/` | Andrés |
| 8 | `feature/jenkins` | Julián | `Jenkinsfile`, `docker/jenkins/` | Andrés |
| — | **Entrega 1 lista (merge antes del 4 de noviembre)** | | | |
| 9 | `feature/kafka-productor` | Julián | `streaming/producer.py`, `tests/smoke/test_stream_smoke.py` | Andrés |
| 10 | `feature/streaming` | Andrés | `streaming/stream_job.py`, `tests/smoke/test_model_version.py` | Julián |
| 11 | `feature/supervivencia` | Andrés | `jobs/survival.py` | Julián |
| 12 | `feature/riesgo` | Julián | `jobs/risk_train.py` | Andrés |
| 13 | `feature/dashboard` | Andrés | `powerbi/` (el .pbix), capturas en `docs/` | Julián |

Los informes (`docs/informe_entrega*.md`) los escriben entre los dos: cada uno redacta las secciones de sus ramas y revisa las del otro.

## Flujo por rama

```bash
git switch main && git pull
git switch -c feature/sentimiento
# copiar los archivos de la tabla, leerlos y probarlos
docker compose --profile test run --rm tests -q tests/unit
git add jobs/sentiment_train.py
git commit -m "Entrena NB y regresión logística con partición por negocio"
git push -u origin feature/sentimiento
```
En GitHub: *Compare & pull request* → descripción (qué hace, cómo se probó) → el otro integrante revisa → *Merge*.

Si dos ramas tocan el mismo archivo, la segunda hace `git pull origin main` antes del PR y resuelve el conflicto.

## Antes de la sustentación

Cada uno debe poder explicar, sin mirar al otro: la fórmula de la tasa y por qué no es una proporción; la partición por negocio; censura y entrada tardía; qué hace cada etapa de Jenkins; cómo llega una reseña desde el productor hasta una alerta. Ver `docs/sustentacion.md`.
