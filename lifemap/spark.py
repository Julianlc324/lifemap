"""Creación de la SparkSession con el MongoDB Spark Connector (y Kafka para streaming)."""
import os

from . import config

PACKAGES = [
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
]


def get_spark(app_name, master=None, cores=None, shuffle_partitions=None, packages=True):
    from pyspark.sql import SparkSession

    master = master or config.SPARK_MASTER
    builder = SparkSession.builder.appName(app_name).master(master)
    if packages:
        builder = (builder.config("spark.jars.packages", ",".join(PACKAGES))
                   .config("spark.jars.ivy", os.environ.get("SPARK_IVY", "/opt/ivy")))
    builder = (
        builder
        .config("spark.mongodb.read.connection.uri", config.MONGO_URI)
        .config("spark.mongodb.write.connection.uri", config.MONGO_URI)
        .config("spark.mongodb.read.database", config.MONGO_DB)
        .config("spark.mongodb.write.database", config.MONGO_DB)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or 64))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
    )
    if master.startswith("spark://"):
        builder = builder.config("spark.cores.max", str(cores or config.SPARK_BATCH_CORES))
        driver_host = os.environ.get("SPARK_DRIVER_HOST")
        if driver_host:
            builder = (builder.config("spark.driver.host", driver_host)
                       .config("spark.driver.bindAddress", "0.0.0.0"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_mongo(spark, collection, pipeline=None):
    reader = spark.read.format("mongodb").option("collection", collection)
    if pipeline:
        import json
        reader = reader.option("aggregation.pipeline", json.dumps(pipeline))
    return reader.load()


def write_mongo(df, collection, mode="append", operation="replace", id_fields="_id"):
    (df.write.format("mongodb").mode(mode)
       .option("collection", collection)
       .option("operationType", operation)
       .option("idFieldList", id_fields)
       .option("upsertDocument", "true" if operation != "update" else "false")
       .save())
