# LifeMap: reputación, ubicación y supervivencia de negocios

Proyecto integrador del curso **Big Data** · Institución Universitaria de Envigado · 2026-2

| | |
|---|---|
| **Equipo** | Julián ([@Julianlc324](https://github.com/Julianlc324)) · Andrés |
| **Área y familia** | Philadelphia · Restaurants (configurable en `config/lifemap.env`) |
| **URL pública** | `https://<SU-DOMINIO>.ngrok-free.app` ← *reemplazar al desplegar* |
| **Estado** | `https://<SU-DOMINIO>.ngrok-free.app/health` |
| **Dataset** | [Yelp Open Dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset) (Kaggle). Fuente: Yelp Inc.; se respetan sus términos de uso. |

Una agencia de desarrollo económico quiere saber cuánto viven los negocios, qué dice su reputación y en qué zonas se concentra el riesgo de cierre, consultable por ubicación y actualizado a medida que llegan reseñas.

## Arquitectura

```
Kaggle API ─► Dask (scheduler + 2 workers) ─► MongoDB (GeoJSON + 2dsphere) ◄─ Spark batch (maestro + 2 workers)
                limpieza por lotes              negocios, reseñas, tasas,        vida, sentimiento (NB vs LR),
                                                ventanas, alertas, curvas        MapReduce, balanceo, KM, riesgo
                                                    ▲             │
Productor acelerado ─► Kafka (KRaft, topic reviews x3) ─► Spark Structured Streaming
                                                    │             │  inferencia, ventanas 30 d + watermark 7 d,
                                                    │             └► alertas con foreachBatch
                                               API Flask ─► ngrok (URL pública) ─► Power BI
GitHub (merge a main) ─► Jenkins (pollSCM 2 min) ─► build ─► pytest ─► API candidata ─► humo streaming ─► despliegue
```

| Servicio (Compose) | Rol |
|---|---|
| `mongo` | Base única: `businesses` (GeoJSON + 2dsphere), `reviews`, `closure_rates`, `cell_sentiment`, `windows`, `alerts`, `survival`, ... |
| `spark-master`, `spark-worker-1/2` | Spark 3.5.3 con MongoDB Spark Connector 10.4 y conector Kafka |
| `dask-scheduler`, `dask-worker-1/2` | Ingesta y limpieza particionada |
| `kafka` (+ `kafka-init`) | Broker KRaft, topic `reviews` con 3 particiones |
| `ingest`, `batch` | Pipeline de una sola ejecución (cada paso deja marca en `meta` y no se repite) |
| `api` | Flask + gunicorn en el puerto 5000 |
| `streaming` | Job de Structured Streaming (espera a que existan los modelos) |
| `jenkins` | CI/CD con Configuration as Code; usa el socket de Docker del servidor |
| `ngrok` (perfil `tunnel`) | Publica la API con un dominio fijo |

## Despliegue desde cero (solo con este README)

**Servidor:** un equipo distinto al portátil de desarrollo (laboratorio o casa), con Docker Engine / Docker Desktop (Compose v2), **16 GB de RAM**, 4+ núcleos y 40 GB libres. Debe quedar encendido y sin suspensión.

1. **Cuentas** (una sola vez):
   - Kaggle → *Settings → API → Create New Token* (usuario + key).
   - ngrok → *Your Authtoken* y *Domains → Create Domain* (dominio estático gratuito).
   - GitHub → token *fine-grained* con permiso de lectura de *Contents* sobre este repo.
2. **Clonar y configurar secretos** (fuera del repositorio):
   ```bash
   git clone https://github.com/Julianlc324/lifemap.git && cd lifemap
   cp .env.example .env      # completar KAGGLE_*, GITHUB_TOKEN, JENKINS_ADMIN_PASSWORD, NGROK_*
   ```
3. **Levantar todo**:
   ```bash
   GIT_COMMIT=$(git rev-parse HEAD) docker compose --profile tunnel up -d --build
   ```
   La primera vez `ingest` descarga el dataset (~4 GB) y `batch` entrena los modelos; según el equipo tarda entre 40 y 120 minutos. Avance: `docker compose logs -f ingest batch`.
4. **Verificar**: `docker compose ps` (todo `healthy`; `ingest`, `batch` y `kafka-init` terminan con `Exited (0)`) y abrir `https://<SU-DOMINIO>.ngrok-free.app/health`.
5. **Jenkins**: `http://<ip-del-servidor>:8081` (usuario `admin`, clave de `.env`). El job `lifemap` ya existe (JCasC) y revisa `main` cada 2 minutos. Desde ese momento **todo despliegue lo hace Jenkins**.

> **pollSCM documentado:** el servidor está detrás de ngrok y solo la API se publica, así que GitHub no alcanza a Jenkins con un webhook. Según la sección 6 de la guía se usa pollSCM cada 2 minutos (`H/2 * * * *`, variable `POLL_SCM_SPEC`). Si Jenkins fuera alcanzable, el trigger `githubPush()` ya está configurado: basta agregar el webhook `http://<host>:8081/github-webhook/`.

Configuración no secreta: `config/lifemap.env` (área, familia, T, umbrales). Secretos: solo en `.env` del servidor y en la credencial `kaggle` de Jenkins (creada por JCasC desde el entorno). Nunca se suben al repositorio.

## Pipeline de Jenkins

`Checkout → Build images → Unit tests → Start services → Data pipeline → API tests (candidate) → Streaming tests → Deploy`

- **Unit tests** incluye `tests/unit/test_metrics.py`, que verifica la fórmula de la tasa de cierre.
- **API tests** levanta `api-test` con la imagen nueva y ejecuta `tests/integration` contra ella; la API pública solo se reemplaza si todo pasa.
- **Streaming tests**: prueba de humo (publica 100 mensajes y verifica que aparecen clasificados en MongoDB) y prueba de que el streaming cargó el modelo versionado.
- **Deploy** reemplaza `api` y comprueba que `/ping` reporta el commit desplegado.
- Repetir pasos del pipeline de datos: *Build with Parameters* → `FORCE_STEPS=sentiment,closure` (o `all`).
- Demostración del bloqueo con un build fallido: ver [`docs/demo_build_fallido.md`](docs/demo_build_fallido.md).

## API

| Endpoint | Descripción |
|---|---|
| `GET /health` | Estado de MongoDB, Spark, Dask, Kafka (con retraso del consumidor) y API; hash del commit |
| `GET /businesses/near?lat&lon&radius&category&limit` | `$near` |
| `GET\|POST /businesses/within?polygon=<GeoJSON>&category` | `$geoWithin` |
| `GET /zones/summary?lat&lon&radius&category&min_days` | `$geoNear` + `$group`: sentimiento medio y tasa de cierre |
| `GET /stats/closure?by=category\|cell&min_days&sort&limit` | Tasas anualizadas con mínimo de días-negocio |
| `GET\|POST /predict` `{"text": "..."}` | Sentimiento de un texto nuevo (inglés) |
| `GET /live/hotspots?lat&lon&radius&window=latest\|AAAA-MM-DD` | Celdas hotspot de la ventana |
| `GET\|POST /alerts?lat&lon&radius` o `?polygon=` | Alertas activas de riesgo de cierre |
| `GET /survival/<categoria>?type=category\|zone\|global` | Curva de Kaplan-Meier con número en riesgo |
| `GET /risk/near?lat&lon&radius&min_prob` | Probabilidad de cierre de negocios cercanos |
| `GET /reports/markdown` | Tablas de resultados para el informe |
| `GET /powerbi/{alerts,hotspots,survival,negative_trend}` | Tablas planas para Power BI |

Ejemplos (centro de Philadelphia):
```bash
URL=https://<SU-DOMINIO>.ngrok-free.app
curl "$URL/businesses/near?lat=39.9526&lon=-75.1652&radius=800&category=Pizza"
curl "$URL/zones/summary?lat=39.9526&lon=-75.1652&radius=2000"
curl "$URL/stats/closure?by=category&min_days=18250&limit=10"
curl -X POST "$URL/predict" -H 'Content-Type: application/json' -d '{"text":"Cold food and rude staff"}'
curl "$URL/survival/Pizza"
```

## Demo de streaming (Entrega 2)

```bash
docker compose run --rm producer --days-per-tick 30 --tick-seconds 10   # un mes cada 10 s
```
Solo publica reseñas posteriores a `T = CUTOFF_T`, en orden de fecha, con clave `business_id`. El dashboard de Power BI ([`docs/powerbi.md`](docs/powerbi.md)) se actualiza con *Actualizar* durante la demo.

## Operación

| Tarea | Comando |
|---|---|
| Ver estado | `docker compose ps` · `curl $URL/health` |
| Repetir un paso | `FORCE_STEPS=closure docker compose run --rm batch` |
| Pruebas locales | `docker compose --profile test run --rm tests -q tests/unit` |
| UIs internas | Spark `:8090`, Dask `:8787`, Jenkins `:8081` (solo red local) |
| Reiniciar el streaming desde cero | `docker compose stop streaming && docker volume rm lifemap_checkpoints` |

## Documentación

- [`docs/decisiones.md`](docs/decisiones.md): definiciones, umbrales y su justificación (mínimo de días-negocio, T, ventana observada, hotspots, umbral de riesgo).
- [`docs/trabajo_en_equipo.md`](docs/trabajo_en_equipo.md): ramas, responsables y flujo de pull requests.
- [`docs/informe_entrega1.md`](docs/informe_entrega1.md) y [`docs/informe_entrega2.md`](docs/informe_entrega2.md): plantillas de informe.
- [`docs/areas.md`](docs/areas.md): otras áreas metropolitanas si hay que cambiar de ciudad.
- [`docs/sustentacion.md`](docs/sustentacion.md): preguntas probables y dónde está cada cosa en el código.

Código propio del equipo. Bibliotecas de terceros: Apache Spark, Dask, Apache Kafka, MongoDB y su conector para Spark, Flask, lifelines, kafka-python, Leaflet (mapa de la página de inicio).
