"""Copia datos/modelos a un candidato para que un build rojo no altere producción."""
import json
import os
import shutil
from pathlib import Path

from lifemap import config, db

source_name = os.environ['SOURCE_DATABASE']
if not config.MONGO_DB.startswith('lifemap_candidate_') or source_name == config.MONGO_DB:
    raise ValueError('El candidato necesita una base exclusiva')
source = db.client()[source_name]
target = db.get_db()
if target.list_collection_names():
    raise ValueError('La base candidata ya existe; no se sobrescribe')
# La primera migración cambia el esquema del checkpoint. No perder historia retenida.
status = source.meta.find_one({"_id": "stream_status"}) or {}
if source.reviews_stream.count_documents({}) and status.get("protocol_version") != 2:
    raise RuntimeError('Hay historial streaming: requiere snapshot coordinado y migración de checkpoints antes de promover')
for name in source.list_collection_names():
    if not name.startswith('system.'):
        list(source[name].aggregate([{'$match': {}}, {'$out': {'db': config.MONGO_DB, 'coll': name}}]))
db.ensure_indexes()
source_models = Path(os.environ.get('SOURCE_MODELS_DIR', '/models'))
models = Path(config.MODELS_DIR)
models.mkdir(parents=True, exist_ok=False)
for name in ('sentiment', 'risk'):
    pointer = source_models / name / 'current.json'
    if pointer.exists():
        info = json.loads(pointer.read_text())
        dest = models / name / info['version']
        shutil.copytree(info['path'], dest)
        info['path'] = str(dest)
        (models / name / 'current.json').write_text(json.dumps(info, indent=2))
source_data = Path(os.environ.get('SOURCE_DATA_DIR', '/data'))
data = Path(config.DATA_DIR)
data.mkdir(parents=True, exist_ok=False)
for name in ('raw', 'clean'):
    original = source_data / name
    if original.exists():
        # Copia independiente: FORCE_STEPS=ingest no puede truncar datos de producción.
        shutil.copytree(original, data / name)
(data / 'reports').mkdir()
print('Candidato preparado:', config.MONGO_DB, config.MODELS_DIR, config.DATA_DIR)

# Streaming se detiene brevemente durante el snapshot; la copia retoma sus offsets.
checkpoint_source = Path(os.environ.get('SOURCE_CHECKPOINT_DIR', '/checkpoints/v2'))
checkpoint_target = Path(os.environ['CHECKPOINT_DIR'])
if status.get('protocol_version') == 2 and checkpoint_source.exists():
    shutil.copytree(checkpoint_source, checkpoint_target)
