"""MapReduce de tasas de cierre (E1.7).

Map:    cada negocio emite (clave, (cierre, días_negocio)) para su categoría principal y su celda.
Reduce: reduceByKey suma cierres y días-negocio por clave.
Tasa:   cierres / (días-negocio / 365) * 100  (lifemap.metrics.closure_rate, probada en Jenkins).
Se valida contra un groupBy de DataFrames y se marca si el grupo supera el mínimo de días-negocio.
También se calcula, con el mismo patrón, el porcentaje de reseñas negativas por celda.
"""
import json
from pathlib import Path

from pyspark.sql import functions as F
from scipy.stats import chi2

from lifemap import config, db, geo
from lifemap.metrics import closure_rate
from lifemap.spark import get_spark, read_mongo


def poisson_ci(k, years, alpha=0.05):
    """IC exacto (Garwood) de la tasa anual en %, para mostrar la incertidumbre de grupos pequeños."""
    if years <= 0:
        return None, None
    low = 0.0 if k == 0 else chi2.ppf(alpha / 2, 2 * k) / 2
    high = chi2.ppf(1 - alpha / 2, 2 * k + 2) / 2
    return round(low / years * 100, 3), round(high / years * 100, 3)


def map_business(row):
    value = (int(row["event"]), int(row["business_days"]))
    return [(("category", row["main_category"]), value), (("cell", row["geohash"]), value)]


def run():
    spark = get_spark("lifemap-closure-rates")
    biz = read_mongo(spark, "businesses", [
        {"$match": {"business_days": {"$exists": True}}},
        {"$project": {"_id": 1, "main_category": 1, "geohash": 1, "event": 1, "business_days": 1}},
    ]).cache()

    # ---------------- MapReduce con RDD
    reduced = dict(biz.rdd.flatMap(map_business)
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                   .collect())

    # ---------------- Validación con groupBy de DataFrames
    check = {}
    for by, col in (("category", "main_category"), ("cell", "geohash")):
        for r in biz.groupBy(col).agg(F.sum("event").alias("c"), F.sum("business_days").alias("d"),
                                      F.count(F.lit(1)).alias("n")).collect():
            check[(by, r[col])] = (int(r["c"]), int(r["d"]), int(r["n"]))
    mismatches = [k for k in check if reduced.get(k) != check[k][:2]]
    assert not mismatches and len(reduced) == len(check), f"MapReduce != groupBy en {mismatches[:5]}"
    print(f"Validación MapReduce vs groupBy: {len(check)} claves idénticas")

    # ---------------- Tasas
    docs = []
    for (by, key), (closures, days) in reduced.items():
        years = days / 365.0
        low, high = poisson_ci(closures, years)
        doc = {"_id": f"{by}:{key}", "by": by, "key": key, "closures": closures,
               "business_days": days, "business_years": round(years, 2),
               "n_businesses": check[(by, key)][2],
               "closure_rate_pct": round(closure_rate(closures, days), 3),
               "ci95_low": low, "ci95_high": high,
               "sufficient": days >= config.MIN_BUSINESS_DAYS,
               "min_business_days": config.MIN_BUSINESS_DAYS}
        if by == "cell":
            lat, lon = geo.center(key)
            doc.update(geometry=geo.polygon(key), center=geo.point(lat, lon))
        docs.append(doc)

    # ---------------- % de reseñas negativas por celda (mismo patrón MapReduce)
    reviews = read_mongo(spark, "reviews", [{"$project": {"geohash": 1, "stars": 1}}])
    neg = dict(reviews.rdd.map(lambda r: (r["geohash"], (1 if r["stars"] <= 2 else 0, 1)))
               .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).collect())
    cell_docs = []
    for gh, (n_neg, n) in neg.items():
        lat, lon = geo.center(gh)
        cell_docs.append({"_id": gh, "geohash": gh, "n_reviews": n, "n_negative": n_neg,
                          "pct_negative": round(100.0 * n_neg / n, 2),
                          "geometry": geo.polygon(gh), "center": geo.point(lat, lon)})

    database = db.get_db()
    database.closure_rates.drop()
    database.closure_rates.insert_many(docs)
    database.cell_sentiment.drop()
    if cell_docs:
        database.cell_sentiment.insert_many(cell_docs)
    db.ensure_indexes()

    total_c = sum(v[0] for k, v in reduced.items() if k[0] == "category")
    total_d = sum(v[1] for k, v in reduced.items() if k[0] == "category")
    summary = {"global_rate_pct": round(closure_rate(total_c, total_d), 3), "closures": total_c,
               "business_days": total_d, "keys": len(docs),
               "sufficient_keys": sum(d["sufficient"] for d in docs), "validated": True}
    Path(config.DATA_DIR, "reports").mkdir(parents=True, exist_ok=True)
    Path(config.DATA_DIR, "reports", "closure_rates.json").write_text(
        json.dumps({"summary": summary, "rates": docs}, indent=2, default=str))
    print("Tasas de cierre:", summary)
    spark.stop()
    return summary
