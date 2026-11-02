# Imagen para el scheduler, los workers de Dask y el job de ingesta (mismo entorno en todos).
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends unzip curl && rm -rf /var/lib/apt/lists/*
COPY requirements-dask.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
WORKDIR /app
COPY lifemap ./lifemap
COPY ingest ./ingest
RUN mkdir -p /data && chmod -R a+rwX /data
