"""Balanceo de carga (E1.8).

1. Tiempos: la operación MapReduce del % de reseñas negativas por celda y por categoría (sobre
   todas las reseñas del área) con 2, 8, 32 y 200 particiones, 3 repeticiones cada una,
   repartiendo sin clave (round-robin) y con clave (categoría principal).
2. Sesgo: filas por partición con spark_partition_id(), sin clave vs con clave (ciudad, categoría).
La entrada se cachea antes de medir para que el tiempo sea de cómputo y shuffle, no de lectura.
"""
import json
import statistics
import time
from pathlib import Path

from pyspark.sql import functions as F

from lifemap import config, db
from lifemap.spark import get_spark

PARTITIONS = [2, 8, 32, 200]
REPS = 3


def mapreduce(df, n, key=None):
    """Operación medida: reparto + map + reduceByKey + collect."""
    parted = df.repartition(n, key) if key else df.repartition(n)
    rdd = parted.rdd
    # Nivel reseña: (clave, (negativa, 1)) por celda y por categoría. La versión por negocio
    # (cierre, días) tiene muy pocas filas para que el número de particiones se note en el tiempo.
    out = (rdd.flatMap(lambda r: [(("cell", r["geohash"]), (1 if r["stars"] <= 2 else 0, 1)),
                                  (("category", r["main_category"]), (1 if r["stars"] <= 2 else 0, 1))])
           .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions=n))
    return len(out.collect())


def partition_counts(df, n, key=None):
    parted = df.repartition(n, key) if key else df.repartition(n)
    rows = parted.groupBy(F.spark_partition_id().alias("pid")).count().collect()
    counts = {r["pid"]: r["count"] for r in rows}
    full = [counts.get(i, 0) for i in range(n)]
    mean = sum(full) / n
    return {
        "partitions": n, "key": key or "sin clave", "rows": sum(full),
        "non_empty": sum(1 for c in full if c > 0), "min": min(full), "max": max(full),
        "mean": round(mean, 1), "std": round(statistics.pstdev(full), 1),
        "skew_max_over_mean": round(max(full) / mean, 2) if mean else None,
        "counts": full,
    }


def run():
    spark = get_spark("lifemap-load-balancing", shuffle_partitions=200)
    spark.conf.set("spark.sql.adaptive.enabled", "false")  # AQE fusionaría particiones

    biz = spark.read.parquet(f"{config.DATA_DIR}/clean/business.parquet").select("business_id", "city")
    reviews = spark.read.parquet(f"{config.DATA_DIR}/clean/reviews").select("business_id", "stars")
    life = (spark.read.format("mongodb").option("collection", "businesses").load()
            .select(F.col("_id").alias("business_id"), "main_category", "geohash", "event", "business_days")
            .filter(F.col("business_days").isNotNull()))
    # Una fila por reseña con los atributos de su negocio: es la carga que se reparte
    data = (reviews.join(life.join(biz, "business_id"), "business_id")
            .select("business_id", "city", "main_category", "geohash", "event", "business_days", "stars")
            .cache())
    n_rows = data.count()
    print("Filas de entrada:", n_rows)

    timings = []
    for strategy, key in (("sin clave", None), ("con clave: main_category", "main_category")):
        for n in PARTITIONS:
            mapreduce(data, n, key)  # calentamiento (no se cuenta)
            times = []
            for _ in range(REPS):
                t0 = time.perf_counter()
                mapreduce(data, n, key)
                times.append(time.perf_counter() - t0)
            timings.append({"strategy": strategy, "partitions": n, "times_s": [round(t, 3) for t in times],
                            "mean_s": round(statistics.mean(times), 3),
                            "std_s": round(statistics.stdev(times), 3), "rows": n_rows})
            print(timings[-1])

    skew = [partition_counts(data, n, key) for n in PARTITIONS for key in (None, "city", "main_category")]
    for s in skew:
        print({k: v for k, v in s.items() if k != "counts"})

    database = db.get_db()
    database.load_balancing.drop()
    database.load_balancing.insert_many([dict(t, kind="timing") for t in timings] +
                                        [dict(s, kind="skew") for s in skew])
    Path(config.DATA_DIR, "reports").mkdir(parents=True, exist_ok=True)
    Path(config.DATA_DIR, "reports", "load_balancing.json").write_text(
        json.dumps({"timings": timings, "skew": skew}, indent=2))
    spark.stop()
    return {"rows": n_rows}
