"""Modelo de sentimiento (E1.6): pipeline de SentimentStream sobre texto real de Yelp.

- Limpieza -> tokenización -> stopwords en inglés (conservando negaciones) -> HashingTF -> IDF.
- Naive Bayes multinomial vs regresión logística multinomial.
- Partición por negocio (hash del business_id): un negocio nunca queda en entrenamiento y prueba.
- Solo reseñas con fecha <= T: las posteriores se reservan para el streaming de la Entrega 2.
- Métricas: precision, recall y F1 por clase, F1 macro, matriz de confusión y distribución de clases.
"""
import json
from datetime import datetime, time as dtime
from pathlib import Path

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import IDF, HashingTF, RegexTokenizer, SQLTransformer, StopWordsRemover
from pyspark.sql import functions as F

from lifemap import config, db, models
from lifemap.spark import get_spark, read_mongo, write_mongo

NEGATIONS = {"no", "not", "nor", "against", "don", "didn", "doesn", "isn", "wasn", "weren",
             "won", "wouldn", "couldn", "shouldn", "aren", "haven", "hasn", "hadn", "never"}
NUM_FEATURES = 1 << 18
TRAIN_PCT = 80


def featurization():
    stop = [w for w in StopWordsRemover.loadDefaultStopWords("english") if w not in NEGATIONS]
    return [
        SQLTransformer(statement=(
            "SELECT *, trim(regexp_replace(regexp_replace(lower(text), 'https?://[^ ]+', ' '), "
            "'[^a-z ]', ' ')) AS clean_text FROM __THIS__")),
        RegexTokenizer(inputCol="clean_text", outputCol="tokens", pattern="\\s+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="filtered", stopWords=stop),
        HashingTF(inputCol="filtered", outputCol="tf", numFeatures=NUM_FEATURES),
        IDF(inputCol="tf", outputCol="features", minDocFreq=5),
    ]


def evaluate(pred, name):
    rows = pred.groupBy("label", "prediction").count().collect()
    labels = [0, 1, 2]
    cm = {(int(r["label"]), int(r["prediction"])): int(r["count"]) for r in rows}
    matrix = [[cm.get((t, p), 0) for p in labels] for t in labels]
    per_class, total, correct = {}, sum(cm.values()), sum(cm.get((c, c), 0) for c in labels)
    for c in labels:
        tp = cm.get((c, c), 0)
        fp = sum(cm.get((t, c), 0) for t in labels if t != c)
        fn = sum(cm.get((c, p), 0) for p in labels if p != c)
        prec = tp / (tp + fp) if tp + fp else 0.0
        rec = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * prec * rec / (prec + rec) if prec + rec else 0.0
        per_class[config.SENTIMENT_LABELS[c]] = {
            "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1, 4),
            "support": tp + fn}
    macro_f1 = sum(v["f1"] for v in per_class.values()) / 3
    weighted_f1 = sum(v["f1"] * v["support"] for v in per_class.values()) / max(total, 1)
    return {
        "classifier": name, "accuracy": round(correct / max(total, 1), 4),
        "macro_f1": round(macro_f1, 4), "weighted_f1": round(weighted_f1, 4),
        "per_class": per_class, "confusion_matrix": matrix,
        "confusion_labels": [config.SENTIMENT_LABELS[c] for c in labels], "n_test": total,
    }


def run():
    spark = get_spark("lifemap-sentiment-train", shuffle_partitions=128)
    t_end = datetime.combine(config.CUTOFF_T, dtime(23, 59, 59)).strftime("%Y-%m-%dT%H:%M:%SZ")
    reviews = read_mongo(spark, "reviews", [
        {"$match": {"date": {"$lte": {"$date": t_end}}}},
        {"$project": {"_id": 1, "business_id": 1, "text": 1, "stars": 1, "date": 1}},
    ])
    data = (reviews
            .withColumn("label", F.when(F.col("stars") <= 2, 0.0)
                        .when(F.col("stars") == 3, 1.0).otherwise(2.0))
            .withColumn("bucket", F.pmod(F.xxhash64("business_id"), F.lit(100)))
            .withColumn("split", F.when(F.col("bucket") < TRAIN_PCT, "train").otherwise("test")))

    # Partición por negocio: se verifica que ningún negocio aparece en ambos lados
    overlap = (data.groupBy("business_id").agg(F.countDistinct("split").alias("k"))
               .filter("k > 1").count())
    assert overlap == 0, f"{overlap} negocios en entrenamiento y prueba"

    dist = {f"{r['split']}|{config.SENTIMENT_LABELS[int(r['label'])]}": r["count"]
            for r in data.groupBy("split", "label").count().collect()}
    train = data.filter("split = 'train'").repartition(64)
    test = data.filter("split = 'test'")

    feat_model = Pipeline(stages=featurization()).fit(train)
    train_f = feat_model.transform(train).select("label", "features").cache()
    test_f = feat_model.transform(test).select("label", "features").cache()
    print("Entrenamiento:", train_f.count(), "| prueba:", test_f.count())

    candidates = {
        "naive_bayes": NaiveBayes(modelType="multinomial", smoothing=1.0),
        "logistic_regression": LogisticRegression(family="multinomial", maxIter=30,
                                                  regParam=0.01, elasticNetParam=0.0),
    }
    results, fitted = [], {}
    for name, est in candidates.items():
        m = est.fit(train_f)
        fitted[name] = m
        res = evaluate(m.transform(test_f), name)
        results.append(res)
        print(json.dumps(res, indent=1))

    best = max(results, key=lambda r: r["macro_f1"])
    full = PipelineModel(stages=list(feat_model.stages) + [fitted[best["classifier"]]])
    version = models.new_version()
    path = str(models.model_root("sentiment") / version)
    full.write().overwrite().save(path)
    pointer = models.register("sentiment", version, {
        "classifier": best["classifier"], "macro_f1": best["macro_f1"],
        "trained_until": config.CUTOFF_T.isoformat(), "split": "por negocio 80/20 (xxhash64)"})

    database = db.get_db()
    database.model_metrics.delete_many({"model": "sentiment"})
    database.model_metrics.insert_many([
        dict(r, model="sentiment", version=version, chosen=(r is best), class_distribution=dist)
        for r in results])
    Path(config.DATA_DIR, "reports").mkdir(parents=True, exist_ok=True)
    Path(config.DATA_DIR, "reports", "sentiment_metrics.json").write_text(
        json.dumps({"results": results, "class_distribution": dist, "chosen": best["classifier"],
                    "version": version}, indent=2, ensure_ascii=False))

    # Sentimiento medio por negocio (predicho, reseñas <= T) -> documento del negocio
    scored = full.transform(data).select("business_id", "prediction")
    per_business = (scored.groupBy("business_id").agg(
        F.avg(F.when(F.col("prediction") == 0, -1.0).when(F.col("prediction") == 1, 0.0)
              .otherwise(1.0)).alias("sentiment_mean"),
        F.avg((F.col("prediction") == 0).cast("double")).alias("pct_negative_pred"),
        F.count(F.lit(1)).alias("n_reviews_T"))
        .withColumn("pct_negative_pred", F.round(F.col("pct_negative_pred") * 100, 2))
        .withColumnRenamed("business_id", "_id"))
    write_mongo(per_business, "businesses", operation="update")
    spark.stop()
    return {"version": pointer["version"], "chosen": best["classifier"], "macro_f1": best["macro_f1"]}
