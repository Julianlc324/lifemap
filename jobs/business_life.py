"""Vida de cada negocio (E1.5), calculada en Spark y guardada en el mismo documento GeoJSON.

Por negocio: primera y última reseña, número de reseñas, días-negocio, evento (is_open = 0),
censura (is_open = 1) y entrada tardía respecto al inicio de la ventana observada (OBS_START).
Misma lógica que lifemap.metrics.business_life (probada con pytest en Jenkins).
"""
from pyspark.sql import functions as F

from lifemap import config, db
from lifemap.spark import get_spark, read_mongo, write_mongo


def run():
    spark = get_spark("lifemap-business-life")
    reviews = read_mongo(spark, "reviews", [{"$project": {"business_id": 1, "date": 1}}])
    businesses = read_mongo(spark, "businesses", [{"$project": {"_id": 1, "is_open": 1}}])

    per_business = reviews.groupBy("business_id").agg(
        F.to_date(F.min("date")).alias("first_review"),
        F.to_date(F.max("date")).alias("last_review"),
        F.count(F.lit(1)).alias("n_reviews"),
    )
    obs = F.lit(config.OBS_START.isoformat()).cast("date")
    life = (
        businesses.join(per_business, businesses["_id"] == per_business["business_id"], "inner")
        .withColumn("business_days", F.datediff("last_review", "first_review") + 1)
        .withColumn("event", (F.col("is_open") == 0).cast("int"))
        .withColumn("censored", 1 - F.col("event"))
        .withColumn("entry_raw", F.greatest(F.lit(0), F.datediff(obs, F.col("first_review"))))
        .withColumn("late_entry", F.col("entry_raw") > 0)
        .withColumn("entry_days", F.least(F.col("entry_raw"), F.col("business_days")))
        .withColumn("in_window", F.col("last_review") >= obs)
        .select("_id", F.col("first_review").cast("timestamp").alias("first_review"),
                F.col("last_review").cast("timestamp").alias("last_review"),
                "n_reviews", "business_days", "event", "censored",
                "late_entry", "entry_days", "in_window")
    ).cache()

    # Fecha de corte del dataset: la última reseña presente (define la censura)
    cutoff = reviews.agg(F.max("date").alias("m")).first()["m"]
    write_mongo(life, "businesses", operation="update")

    stats = life.agg(
        F.count(F.lit(1)).alias("negocios"),
        F.sum("event").alias("cierres"),
        F.sum("censored").alias("censurados"),
        F.sum(F.col("late_entry").cast("int")).alias("entrada_tardia"),
        F.sum("business_days").alias("dias_negocio"),
        F.expr("percentile_approx(business_days, 0.5)").alias("mediana_dias"),
    ).first().asDict()
    stats = {k: int(v) for k, v in stats.items()}
    db.set_meta("dataset", cutoff_date=cutoff, life_stats=stats, obs_start=config.OBS_START.isoformat())
    print("Vida de negocios:", stats, "| corte del dataset:", cutoff)
    spark.stop()
    return {"stats": stats}
