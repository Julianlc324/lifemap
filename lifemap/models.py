"""Registro de modelos versionados en /models/<nombre>/<version> con puntero current.json.

La Entrega 2 carga el modelo de la Entrega 1 por este puntero (sin reentrenar), y una prueba
de Jenkins verifica que la versión cargada por el streaming coincide con la registrada.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from . import config


def model_root(name):
    return Path(config.MODELS_DIR) / name


def new_version():
    return datetime.now(timezone.utc).strftime("v%Y%m%d-%H%M%S")


def register(name, version, info):
    root = model_root(name)
    root.mkdir(parents=True, exist_ok=True)
    pointer = {"name": name, "version": version, "path": str(root / version),
               "registered_at": datetime.now(timezone.utc).isoformat(), **info}
    tmp = root / "current.json.tmp"
    tmp.write_text(json.dumps(pointer, indent=2, default=str))
    os.replace(tmp, root / "current.json")
    try:
        from .db import get_db
        get_db().model_registry.insert_one(dict(pointer))
    except Exception as exc:  # noqa: BLE001
        print("Aviso: no se pudo registrar en MongoDB:", exc)
    return pointer


def current(name):
    path = model_root(name) / "current.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def load_pipeline(name):
    """Carga el PipelineModel vigente (requiere una SparkSession activa)."""
    from pyspark.ml import PipelineModel

    pointer = current(name)
    if pointer is None:
        raise FileNotFoundError(f"No hay modelo '{name}' registrado en {model_root(name)}")
    return PipelineModel.load(pointer["path"]), pointer
