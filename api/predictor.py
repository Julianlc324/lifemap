"""/predict: carga perezosa del PipelineModel de sentimiento en un Spark local dentro de la API.

Se recarga solo si cambia la versión registrada en /models/sentiment/current.json.
"""
import threading

from lifemap import config, models

_lock = threading.Lock()
_state = {"spark": None, "model": None, "version": None}


def _ensure():
    pointer = models.current("sentiment")
    if pointer is None:
        raise FileNotFoundError("Aún no hay modelo de sentimiento entrenado (paso 'sentiment' del pipeline)")
    if _state["model"] is not None and _state["version"] == pointer["version"]:
        return
    from lifemap.spark import get_spark

    if _state["spark"] is None:
        _state["spark"] = get_spark("lifemap-api-predict", master="local[2]", shuffle_partitions=1,
                                    packages=False)
    model, pointer = models.load_pipeline("sentiment")
    _state.update(model=model, version=pointer["version"], classifier=pointer.get("classifier"))


def predict(texts):
    from pyspark.ml.functions import vector_to_array

    with _lock:
        _ensure()
        df = _state["spark"].createDataFrame([(t,) for t in texts], ["text"])
        rows = (_state["model"].transform(df)
                .select("text", "prediction", vector_to_array("probability").alias("p")).collect())
    out = []
    for r in rows:
        probs = {config.SENTIMENT_LABELS[i]: round(float(p), 4) for i, p in enumerate(r["p"])}
        out.append({"text": r["text"], "label": config.SENTIMENT_LABELS[int(r["prediction"])],
                    "label_id": int(r["prediction"]), "probability": max(probs.values()),
                    "probabilities": probs})
    return out, {"version": _state["version"], "classifier": _state.get("classifier")}
