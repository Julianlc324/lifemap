# Imagen única para Spark (maestro, workers), jobs batch, streaming, productor y API.
# Usar la misma imagen en driver y executors garantiza la misma versión de Python.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY requirements-spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    SPARK_IVY=/opt/ivy \
    PYTHONUNBUFFERED=1

# Descarga en tiempo de construcción el MongoDB Spark Connector y el conector de Kafka
# (quedan en /opt/ivy; así el despliegue no depende de Maven Central).
COPY docker/spark/warm_packages.py /tmp/warm_packages.py
RUN mkdir -p /opt/ivy && python3 /tmp/warm_packages.py && chmod -R a+rwX /opt/ivy

WORKDIR /app
COPY lifemap ./lifemap
COPY jobs ./jobs
COPY streaming ./streaming
COPY api ./api
COPY tests ./tests
COPY bin ./bin
COPY pytest.ini ./
RUN (chmod +x bin/*.sh 2>/dev/null || true) && mkdir -p /data /models /checkpoints && chmod -R a+rwX /data /models /checkpoints

ARG GIT_COMMIT=dev
ENV GIT_COMMIT=${GIT_COMMIT}
