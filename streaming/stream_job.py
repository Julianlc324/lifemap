"""Spark Structured Streaming (E2.3, E2.4, E2.7).

Query 1 "reviews" (foreachBatch):
  - Clasifica cada reseña con el modelo de sentimiento de la Entrega 1 (cargado, nunca reentrenado)
    y la escribe en `reviews_stream` con predicción, probabilidad y marca de procesamiento.
  - Actualiza el estado de los negocios que recibieron reseñas, recalcula sus variables, los
    puntúa con el modelo de riesgo y guarda en `alerts` (con geometría) los que superan el umbral.
  - Registra los offsets procesados por partición (retraso del consumidor en /health).
Query 2 "windows": ventanas de 30 días sobre el tiempo del evento (campo `date`) con watermark
  de 7 días, por celda geohash; conteo y % negativo; cada ventana con la geometría de su celda.
  Hotspot: el % negativo sube >= HOTSPOT_DELTA_PP puntos frente a la ventana anterior, con al
  menos HOTSPOT_MIN_REVIEWS reseñas en ambas ventanas.
"""
import time
import os
from datetime import datetime, timedelta, timezone

from pymongo import UpdateOne
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from lifemap import config, db, features, geo, models
from lifemap.spark import get_spark, read_mongo
from lifemap.metrics import cutoff_exclusive
from streaming.persistence import rebuild_state, refresh_hotspots

SCHEMA = StructType([
    StructField("review_id", StringType()), StructField("business_id", StringType()),
    StructField("stars", IntegerType()), StructField("text", StringType()),
    StructField("date", StringType()), StructField("produced_at", StringType()),
])
CHECKPOINTS = os.environ.get("CHECKPOINT_DIR", "/checkpoints/v2")


def wait_for_models():
    while True:
        s, r = models.current("sentiment"), models.current("risk")
        if s and r:
            return s, r
        print("Esperando modelos registrados (pipeline batch)...", {"sentiment": bool(s), "risk": bool(r)})
        time.sleep(30)


class Scorer:
    """Actualiza estados de negocio y emite alertas dentro de cada micro-lote."""

    def __init__(self, spark, risk_model, risk_pointer, sent_pointer):
        self.spark, self.model, self.pointer = spark, risk_model, risk_pointer
        self.threshold = float(risk_pointer["threshold"])
        self.versions = {"sentiment": sent_pointer["version"], "risk": risk_pointer["version"]}

    def _new_state(self, database, bid):
        b = database.businesses.find_one({"_id": bid}, {"name": 1, "location": 1, "main_category": 1, "geohash": 1})
        if not b:
            return None
        res = list(database.risk_features.aggregate([
            {"$geoNear": {"near": b["location"], "distanceField": "d", "spherical": True,
                          "maxDistance": config.COMPETITOR_RADIUS_M,
                          "query": {"main_category": b.get("main_category"), "active_at_T": True, "_id": {"$ne": bid}}}},
            {"$count": "n"}]))
        return {"_id": bid, "name": b.get("name"), "location": b["location"],
                "main_category": b.get("main_category"), "geohash": b.get("geohash"),
                "n_reviews": 0, "sent_sum": 0.0, "stars_sum": 0.0, "monthly": {},
                "competitors_500m": res[0]["n"] if res else 0}

    def process(self, rows):
        if not rows:
            return 0
        database = db.get_db()
        ids = list({r["business_id"] for r in rows})
        baselines = {d["_id"]: d for d in database.risk_features.find({"_id": {"$in": ids}})}
        states = {}
        for bid in ids:
            baseline = baselines.get(bid) or self._new_state(database, bid)
            if baseline is None:
                continue
            if baseline.get("baseline_cutoff") and baseline["baseline_cutoff"] != config.CUTOFF_T.isoformat():
                raise ValueError("Estado inicial corresponde a otro corte T")
            reviews = database.reviews_stream.find({"business_id": bid}).sort([("date", 1), ("_id", 1)])
            states[bid] = rebuild_state(baseline, reviews)
        if not states:
            return 0
        # Referencia monotónica por negocio; un lote tardío no hace retroceder la edad.
        refs = {bid: datetime.fromisoformat(st["last_review"]) for bid, st in states.items()}
        feats = [{"business_id": bid, **features.compute_features(st, refs[bid])}
                 for bid, st in states.items() if bid in ids]
        scored = (self.model.transform(self.spark.createDataFrame(feats))
                  .withColumn("p1", vector_to_array("probability")[1])
                  .select("business_id", "p1").collect())
        now = datetime.now(timezone.utc)
        state_ops, score_ops, alert_ops = [], [], []
        for st in states.values():
            st["ref_date"] = refs[st["_id"]].date().isoformat()
            body = {k: v for k, v in st.items() if k != "_id"}
            state_ops.append(UpdateOne({"_id": st["_id"]}, {"$set": body}, upsert=True))
        feats_by_id = {f["business_id"]: f for f in feats}
        n_alerts = 0
        for row in scored:
            bid, p = row["business_id"], float(row["p1"])
            st = states[bid]
            base = {"name": st.get("name"), "location": st["location"], "main_category": st.get("main_category"),
                    "geohash": st.get("geohash"), "probability": round(p, 4), "threshold": self.threshold,
                    "event_date": refs[bid], "model_version": self.versions["risk"], "updated_at": now}
            score_ops.append(UpdateOne({"_id": bid}, {"$set": {**base, "source": "stream",
                                                                "above_threshold": p >= self.threshold}}, upsert=True))
            if p >= self.threshold:
                n_alerts += 1
                alert_ops.append(UpdateOne({"_id": bid}, {
                    "$set": {**base, "active": True, "features": feats_by_id[bid]},
                    "$setOnInsert": {"created_at": now}}, upsert=True))
            else:
                alert_ops.append(UpdateOne({"_id": bid}, {"$set": {"active": False, "updated_at": now,
                                                                   "probability": round(p, 4)}}))
        if state_ops:
            database.risk_state.bulk_write(state_ops, ordered=False)
        if score_ops:
            database.risk_scores.bulk_write(score_ops, ordered=False)
        if alert_ops:
            database.alerts.bulk_write(alert_ops, ordered=False)
        return n_alerts


def main():
    sent_pointer, risk_pointer = wait_for_models()
    spark = get_spark("lifemap-streaming", cores=config.SPARK_STREAM_CORES, shuffle_partitions=8)
    sentiment_model, sent_pointer = models.load_pipeline("sentiment")
    risk_model, risk_pointer = models.load_pipeline("risk")
    if risk_pointer.get("sentiment_version") != sent_pointer["version"]:
        raise ValueError("El modelo de riesgo requiere otra versión de sentimiento")
    if sent_pointer.get("trained_until") != config.CUTOFF_T.isoformat():
        raise ValueError("El modelo de sentimiento no corresponde a T")
    db.set_meta("stream_status", sentiment_version=sent_pointer["version"],
                risk_version=risk_pointer["version"], started_at=datetime.now(timezone.utc),
                retrained=False, commit=config.GIT_COMMIT, protocol_version=2)
    print("Modelos cargados (sin reentrenar):", sent_pointer["version"], risk_pointer["version"])
    db.ensure_indexes()

    biz = F.broadcast(read_mongo(spark, "businesses", [
        {"$project": {"_id": 1, "geohash": 1, "main_category": 1, "latitude": 1, "longitude": 1}}])
        .withColumnRenamed("_id", "business_id").cache())

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", config.KAFKA_TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 5000)
           .option("failOnDataLoss", "true").load())
    parsed = (raw.select(F.col("partition"), F.col("offset"),
                         F.from_json(F.col("value").cast("string"), SCHEMA).alias("m"))
              .select("partition", "offset", "m.*")
              .withColumn("event_time", F.to_timestamp("date"))
              .filter(F.col("event_time").isNotNull())
              .filter(F.col("event_time") >= F.lit(cutoff_exclusive(config.CUTOFF_T)))
              .filter(F.col("review_id").isNotNull() & (F.length("review_id") > 0))
              .filter(F.col("business_id").isNotNull())
              .filter(F.length(F.trim("text")) > 0)
              .filter(F.col("stars").between(1, 5)))
    enriched = parsed.join(biz, "business_id", "left")
    classified = (sentiment_model.transform(enriched)
                  .withColumn("probs", vector_to_array("probability"))
                  .withColumn("probability_max", F.array_max("probs"))
                  .withColumn("is_negative", (F.col("prediction") == 0).cast("int"))
                  .select("partition", "offset", "review_id", "business_id", "stars", "text",
                          "event_time", "produced_at", "geohash", "main_category", "latitude",
                          "longitude", "prediction", "probs", "probability_max", "is_negative"))

    scorer = Scorer(spark, risk_model, risk_pointer, sent_pointer)
    labels = config.SENTIMENT_LABELS

    def process_reviews(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        now = datetime.now(timezone.utc)
        database = db.get_db()
        ops, offsets = [], {}
        for r in rows:
            doc = {"review_id": r["review_id"], "business_id": r["business_id"], "stars": r["stars"],
                   "text": r["text"], "date": r["event_time"], "geohash": r["geohash"],
                   "main_category": r["main_category"],
                   "prediction": labels[int(r["prediction"])], "prediction_id": int(r["prediction"]),
                   "probability": round(float(r["probability_max"]), 4),
                   "probabilities": {labels[i]: round(float(p), 4) for i, p in enumerate(r["probs"])},
                   "produced_at": r["produced_at"], "processed_at": now, "batch_id": batch_id,
                   "model_version": sent_pointer["version"]}
            if r["latitude"] is not None:
                doc["location"] = geo.point(r["latitude"], r["longitude"])
            ops.append(UpdateOne({"_id": r["review_id"]}, {"$set": doc}, upsert=True))
            p = str(r["partition"])
            offsets[p] = max(offsets.get(p, 0), int(r["offset"]) + 1)
        database.reviews_stream.bulk_write(ops, ordered=False)
        n_alerts = scorer.process([r.asDict() for r in rows if r["geohash"] is not None])
        database.stream_progress.update_one(
            {"_id": config.KAFKA_TOPIC},
            {"$max": {f"offsets.{p}": o for p, o in offsets.items()},
             "$set": {"updated_at": now, "last_batch_id": batch_id, "model_versions": scorer.versions}}, upsert=True)
        print(f"lote {batch_id}: {len(rows)} reseñas, {n_alerts} negocios sobre el umbral")

    # Dos consultas independientes: deduplicación durable por evento/ID para ventanas.
    # Append finaliza ventanas después del watermark; permite encadenar estados en Spark 3.5.
    unique = (classified.filter(F.col("geohash").isNotNull())
              .withWatermark("event_time", config.WATERMARK_DELAY)
              .dropDuplicates(["review_id", "event_time"]))
    windows = (unique.groupBy(F.window("event_time", config.WINDOW_DURATION).alias("w"), "geohash")
               .agg(F.count(F.lit(1)).alias("count"), F.sum("is_negative").alias("negative")))

    def process_windows(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        database = db.get_db()
        now = datetime.now(timezone.utc)
        ops = []
        for r in rows:
            start, end, gh = r["w"]["start"], r["w"]["end"], r["geohash"]
            pct = round(100.0 * r["negative"] / r["count"], 2) if r["count"] else 0.0
            lat, lon = geo.center(gh)
            ops.append(UpdateOne({"_id": f"{gh}|{start:%Y-%m-%d}"}, {"$set": {
                "geohash": gh, "window_start": start, "window_end": end, "count": int(r["count"]),
                "negative": int(r["negative"]), "pct_negative": pct,
                "geometry": geo.polygon(gh), "center": geo.point(lat, lon),
                "updated_at": now, "batch_id": batch_id}}, upsert=True))
        database.windows.bulk_write(ops, ordered=False)
        refresh_hotspots(database, [r["geohash"] for r in rows])

    q1 = (classified.writeStream.queryName("reviews").foreachBatch(process_reviews)
          .option("checkpointLocation", f"{CHECKPOINTS}/reviews").trigger(processingTime="5 seconds").start())
    q2 = (windows.writeStream.queryName("windows").outputMode("append").foreachBatch(process_windows)
          .option("checkpointLocation", f"{CHECKPOINTS}/windows").trigger(processingTime="5 seconds").start())
    print("Streaming iniciado:", q1.id, q2.id)
    try:
        while True:
            for query in (q1, q2):
                if not query.isActive:
                    raise RuntimeError(f"Consulta detenida: {query.name}: {query.exception()}")
            db.set_meta("stream_status", heartbeat_at=datetime.now(timezone.utc), active=True,
                        active_queries=[q1.name, q2.name])
            spark.streams.awaitAnyTermination(10)
    finally:
        db.set_meta("stream_status", active=False)
        for query in (q1, q2):
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
