"""Modelo de riesgo de cierre (E2.6): patrón de KafkaMed aplicado a negocios.

Variables (solo con datos hasta T; ver lifemap/features.py):
  sentimiento medio predicho, tendencia del % negativo en 6 meses, estrellas medias de reseñas,
  número de reseñas, edad, recencia y competidores de la misma categoría a 500 m ($geoNear).
  No se usan `stars`, `review_count` ni `is_open` del business.json como variables: son una
  fotografía al final del dataset y filtrarían información posterior a T.
Etiqueta (lifemap.metrics.risk_label): 1 si cerró y su última reseña cae en (T, T+12 meses];
  0 si sigue activo después o sigue abierto; se excluyen los negocios sin actividad después de T.
Partición: una fila por negocio, estratificada 60/20/20 (entrenamiento/validación/prueba).
Métricas: AUC-ROC, AUC-PR (frente a la prevalencia como línea base) y recall en el umbral elegido
  en validación maximizando F2 (en alerta temprana pesa más no perder cierres).
"""
import json
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta
from pathlib import Path

from pymongo import UpdateOne
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from lifemap import config, db, features, models
from lifemap.metrics import risk_label, cutoff_exclusive
from lifemap.spark import get_spark, read_mongo


def build_states(spark):
    """Acumulados por negocio con reseñas <= T, puntuadas con el modelo de sentimiento vigente."""
    sentiment, pointer = models.load_pipeline("sentiment")
    t_end = cutoff_exclusive(config.CUTOFF_T).strftime("%Y-%m-%dT%H:%M:%SZ")
    reviews = read_mongo(spark, "reviews", [
        {"$match": {"date": {"$lt": {"$date": t_end}}}},
        {"$project": {"_id": 1, "business_id": 1, "text": 1, "stars": 1, "date": 1}}])
    scored = (sentiment.transform(reviews)
              .select("business_id", "stars", "date", "prediction")
              .withColumn("score", F.when(F.col("prediction") == 0, -1.0)
                          .when(F.col("prediction") == 1, 0.0).otherwise(1.0))
              .withColumn("neg", (F.col("prediction") == 0).cast("int"))
              .withColumn("ym", F.date_format("date", "yyyy-MM")))
    totals = scored.groupBy("business_id").agg(
        F.count(F.lit(1)).alias("n_reviews"), F.sum("score").alias("sent_sum"),
        F.sum("stars").alias("stars_sum"),
        F.date_format(F.min("date"), "yyyy-MM-dd").alias("first_review"),
        F.date_format(F.max("date"), "yyyy-MM-dd").alias("last_review"))
    since = (config.CUTOFF_T - timedelta(days=31 * features.KEEP_MONTHS)).isoformat()
    monthly = (scored.filter(F.col("date") >= F.lit(since).cast("timestamp"))
               .groupBy("business_id", "ym").agg(F.count(F.lit(1)).alias("n"), F.sum("neg").alias("neg"))
               .groupBy("business_id").agg(F.map_from_entries(F.collect_list(
                   F.struct("ym", F.array(F.col("n").cast("long"), F.col("neg").cast("long"))))).alias("monthly")))
    rows = totals.join(monthly, "business_id", "left").collect()
    return {r["business_id"]: {
        "n_reviews": int(r["n_reviews"]), "sent_sum": float(r["sent_sum"]),
        "stars_sum": float(r["stars_sum"]), "first_review": r["first_review"],
        "last_review": r["last_review"], "monthly": dict(r["monthly"] or {}),
    } for r in rows}, pointer


def competitor_density(database, docs):
    """Competidores de la misma categoría activos en T a menos de 500 m, con $geoNear."""
    def count(doc):
        res = list(database.risk_features.aggregate([
            {"$geoNear": {"near": doc["location"], "distanceField": "d", "spherical": True,
                          "maxDistance": config.COMPETITOR_RADIUS_M,
                          "query": {"main_category": doc["main_category"], "active_at_T": True,
                                    "_id": {"$ne": doc["_id"]}}}},
            {"$count": "n"}]))
        return doc["_id"], (res[0]["n"] if res else 0)

    with ThreadPoolExecutor(max_workers=8) as pool:
        return dict(pool.map(count, docs))


def best_threshold(scores_labels, beta=2.0):
    pairs = sorted(scores_labels, key=lambda x: -x[0])
    pos = sum(y for _, y in pairs)
    best, tp, fp = (0.5, -1.0), 0, 0
    for i, (s, y) in enumerate(pairs):
        tp += y
        fp += 1 - y
        if i + 1 < len(pairs) and pairs[i + 1][0] == s:
            continue
        prec, rec = tp / (tp + fp), tp / pos if pos else 0
        if prec + rec:
            fb = (1 + beta ** 2) * prec * rec / (beta ** 2 * prec + rec)
            if fb > best[1]:
                best = (s, fb)
    return best[0]


def binary_metrics(pred_df, threshold):
    ev_roc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="p1", metricName="areaUnderROC")
    ev_pr = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="p1", metricName="areaUnderPR")
    rows = pred_df.select("label", "p1").collect()
    tp = sum(1 for r in rows if r["label"] == 1 and r["p1"] >= threshold)
    fp = sum(1 for r in rows if r["label"] == 0 and r["p1"] >= threshold)
    fn = sum(1 for r in rows if r["label"] == 1 and r["p1"] < threshold)
    tn = len(rows) - tp - fp - fn
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {"auc_roc": round(ev_roc.evaluate(pred_df), 4), "auc_pr": round(ev_pr.evaluate(pred_df), 4),
            "prevalence": round((tp + fn) / max(len(rows), 1), 4), "threshold": float(threshold),
            "precision": round(prec, 4), "recall": round(rec, 4),
            "f1": round(2 * prec * rec / (prec + rec), 4) if prec + rec else 0.0,
            "confusion": {"tp": tp, "fp": fp, "fn": fn, "tn": tn}, "n": len(rows)}


def run():
    spark = get_spark("lifemap-risk-train")
    database = db.get_db()
    states, sent_pointer = build_states(spark)
    t = config.CUTOFF_T

    biz = {d["_id"]: d for d in database.businesses.find(
        {}, {"name": 1, "location": 1, "main_category": 1, "geohash": 1, "is_open": 1, "last_review": 1})}
    docs = []
    for bid, st in states.items():
        b = biz.get(bid)
        if not b:
            continue
        active = (t - datetime.fromisoformat(st["last_review"]).date()).days <= 365
        docs.append({"_id": bid, "name": b.get("name"), "location": b["location"],
                     "main_category": b.get("main_category"), "geohash": b.get("geohash"),
                     "active_at_T": active, "ref_date": t.isoformat(),
                     "sentiment_version": sent_pointer["version"], "baseline_cutoff": t.isoformat(), **st})
    database.risk_features.drop()
    database.risk_features.insert_many(docs)
    db.ensure_indexes()
    density = competitor_density(database, docs)

    rows, by_id = [], {}
    for d in docs:
        d["competitors_500m"] = density.get(d["_id"], 0)
        by_id[d["_id"]] = d
        b = biz[d["_id"]]
        label = risk_label(b["is_open"], b["last_review"], t) if b.get("last_review") else None
        if label is None:
            continue
        feats = features.compute_features(d, t)
        rows.append({"business_id": d["_id"], "label": float(label), **feats})
    if density:
        database.risk_features.bulk_write([UpdateOne({"_id": k}, {"$set": {"competitors_500m": v}})
                                           for k, v in density.items()], ordered=False)

    # Partición estratificada por negocio 60/20/20
    rng = random.Random(42)
    for cls in (0.0, 1.0):
        group = sorted((r for r in rows if r["label"] == cls), key=lambda r: r["business_id"])
        if len(group) < 5:
            raise ValueError("Cada clase necesita al menos 5 negocios para train/valid/test")
        rng.shuffle(group)
        a, b2 = int(0.6 * len(group)), int(0.8 * len(group))
        for i, r in enumerate(group):
            r["split"] = "train" if i < a else ("val" if i < b2 else "test")
    n_pos = sum(r["label"] for r in rows if r["split"] == "train")
    n_neg = sum(1 for r in rows if r["split"] == "train") - n_pos
    w_pos = n_neg / max(n_pos, 1)
    for r in rows:
        r["weight"] = w_pos if r["label"] == 1 else 1.0
    df = spark.createDataFrame(rows).cache()
    print(f"Negocios etiquetados: {len(rows)} | positivos: {int(sum(r['label'] for r in rows))}")

    assembler = VectorAssembler(inputCols=features.FEATURE_NAMES, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    candidates = {
        "logistic_regression": LogisticRegression(weightCol="weight", maxIter=100, regParam=0.01),
        "gbt": GBTClassifier(weightCol="weight", maxDepth=4, maxIter=80, stepSize=0.1, seed=42),
    }
    train, val, test = (df.filter(F.col("split") == s) for s in ("train", "val", "test"))
    results = []
    for name, clf in candidates.items():
        model = Pipeline(stages=[assembler, scaler, clf]).fit(train)
        vpred = model.transform(val).withColumn("p1", vector_to_array("probability")[1])
        thr = best_threshold([(r["p1"], int(r["label"])) for r in vpred.select("p1", "label").collect()])
        tpred = model.transform(test).withColumn("p1", vector_to_array("probability")[1])
        res = {"classifier": name, "validation": binary_metrics(vpred, thr), "test": binary_metrics(tpred, thr)}
        results.append((res, model))
        print(json.dumps(res, indent=1))

    best, best_model = max(results, key=lambda x: x[0]["validation"]["auc_pr"])
    version = models.new_version()
    best_model.write().overwrite().save(str(models.model_root("risk") / version))
    models.register("risk", version, {
        "classifier": best["classifier"], "threshold": best["validation"]["threshold"],
        "features": features.FEATURE_NAMES, "cutoff_T": t.isoformat(),
        "sentiment_version": sent_pointer["version"], "test": best["test"]})
    database.model_metrics.delete_many({"model": "risk"})
    database.model_metrics.insert_many([dict(r, model="risk", version=version,
                                             chosen=r["classifier"] == best["classifier"]) for r, _ in results])

    # Puntaje base en T para todos los negocios con estado (las alertas las emite el streaming)
    all_rows = [{"business_id": k, **features.compute_features(v, t)} for k, v in by_id.items()]
    scored = (best_model.transform(spark.createDataFrame(all_rows))
              .withColumn("p1", vector_to_array("probability")[1]).select("business_id", "p1").collect())
    database.risk_scores.drop()
    database.risk_scores.insert_many([{
        "_id": r["business_id"], "probability": round(float(r["p1"]), 4),
        "above_threshold": float(r["p1"]) >= best["validation"]["threshold"],
        "location": by_id[r["business_id"]]["location"], "name": by_id[r["business_id"]]["name"],
        "main_category": by_id[r["business_id"]]["main_category"], "as_of": t.isoformat(),
        "source": "batch_T", "model_version": version} for r in scored])
    db.ensure_indexes()
    Path(config.DATA_DIR, "reports").mkdir(parents=True, exist_ok=True)
    Path(config.DATA_DIR, "reports", "risk_metrics.json").write_text(
        json.dumps([r for r, _ in results], indent=2))
    spark.stop()
    return {"version": version, "chosen": best["classifier"], "test": best["test"]}
