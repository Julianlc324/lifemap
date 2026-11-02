"""Descarga los paquetes Maven de los conectores en tiempo de construcción de la imagen."""
from pyspark.sql import SparkSession

from_packages = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
])
spark = (SparkSession.builder.master("local[1]").appName("warm")
         .config("spark.jars.packages", from_packages)
         .config("spark.jars.ivy", "/opt/ivy").getOrCreate())
print("Spark", spark.version, "con conectores listos")
spark.stop()
