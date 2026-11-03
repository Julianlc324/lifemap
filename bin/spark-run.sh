#!/usr/bin/env bash
# Ejecuta un script PySpark como driver contra el clúster spark://spark-master:7077.
# Los executors se conectan de vuelta al driver por su IP en la red de Compose.
set -euo pipefail
export SPARK_DRIVER_HOST="$(hostname -i | awk '{print $1}')"
exec python3 "$@"
