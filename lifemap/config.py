"""Configuración centralizada. Todo se lee de variables de entorno (config/lifemap.env).

Nada secreto vive aquí: las credenciales de Kaggle llegan como variables de entorno
inyectadas por Jenkins (credencial `kaggle`) o por el .env local, que está en .gitignore.
"""
import os
from datetime import date


def _env(name, default=None):
    value = os.environ.get(name)
    return default if value in (None, "") else value


def _bbox(raw):
    lat_min, lat_max, lon_min, lon_max = (float(x) for x in raw.split(","))
    if not (-90 <= lat_min < lat_max <= 90 and -180 <= lon_min < lon_max <= 180):
        raise ValueError(f"METRO_BBOX inválido: {raw}")
    return lat_min, lat_max, lon_min, lon_max


# --- Alcance del equipo (área metropolitana + familia de categorías) ---
METRO_NAME = _env("METRO_NAME", "Philadelphia")
METRO_BBOX = _bbox(_env("METRO_BBOX", "39.70,40.30,-75.60,-74.90"))
CATEGORY_FAMILY = _env("CATEGORY_FAMILY", "Restaurants")
# Categorías demasiado genéricas para ser "categoría principal"
GENERIC_CATEGORIES = {c.strip() for c in _env(
    "GENERIC_CATEGORIES",
    "Restaurants,Food,Nightlife,Bars,Shopping,Beauty & Spas,Active Life,Event Planning & Services,"
    "Arts & Entertainment,Hotels & Travel,Local Services,Home Services,Automotive,Health & Medical",
).split(",")}

# --- Rutas y servicios ---
DATA_DIR = _env("DATA_DIR", "/data")
MODELS_DIR = _env("MODELS_DIR", "/models")
MONGO_URI = _env("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = _env("MONGO_DB", "lifemap")
SPARK_MASTER = _env("SPARK_MASTER", "spark://spark-master:7077")
SPARK_MASTER_UI = _env("SPARK_MASTER_UI", "http://spark-master:8080")
DASK_SCHEDULER = _env("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
DASK_DASHBOARD = _env("DASK_DASHBOARD", "http://dask-scheduler:8787")
KAFKA_BOOTSTRAP = _env("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = _env("KAFKA_TOPIC", "reviews")
KAFKA_PARTITIONS = int(_env("KAFKA_PARTITIONS", "3"))
KAGGLE_DATASET = _env("KAGGLE_DATASET", "yelp-dataset/yelp-dataset")

# --- Muestreo (la guía permite >= 1.000.000 reseñas si no se procesa todo) ---
MAX_REVIEWS = int(_env("MAX_REVIEWS", "0"))  # 0 = todas las reseñas del área

# --- Definiciones analíticas ---
GEOHASH_PRECISION = int(_env("GEOHASH_PRECISION", "5"))  # ~4,9 km x 4,9 km
CUTOFF_T = date.fromisoformat(_env("CUTOFF_T", "2020-06-30"))  # fecha de corte T (Entrega 2)
OBS_START = date.fromisoformat(_env("OBS_START", "2010-01-01"))  # inicio de la ventana observada
# Mínimo de días-negocio para comparar tasas: 50 años-negocio (ver docs/decisiones.md)
MIN_BUSINESS_DAYS = int(_env("MIN_BUSINESS_DAYS", str(50 * 365)))
# Kaplan-Meier: no se reportan curvas de grupos pequeños
KM_MIN_N = int(_env("KM_MIN_N", "30"))
KM_MIN_EVENTS = int(_env("KM_MIN_EVENTS", "10"))

# --- Streaming ---
WINDOW_DURATION = _env("WINDOW_DURATION", "30 days")
WATERMARK_DELAY = _env("WATERMARK_DELAY", "7 days")
HOTSPOT_DELTA_PP = float(_env("HOTSPOT_DELTA_PP", "10"))  # sube >= 10 puntos porcentuales
HOTSPOT_MIN_REVIEWS = int(_env("HOTSPOT_MIN_REVIEWS", "15"))
COMPETITOR_RADIUS_M = float(_env("COMPETITOR_RADIUS_M", "500"))
SPARK_STREAM_CORES = _env("SPARK_STREAM_CORES", "2")
SPARK_BATCH_CORES = _env("SPARK_BATCH_CORES", "4")

GIT_COMMIT = _env("GIT_COMMIT", "dev")

SENTIMENT_LABELS = {0: "negativo", 1: "neutral", 2: "positivo"}
SENTIMENT_SCORE = {0: -1.0, 1: 0.0, 2: 1.0}
