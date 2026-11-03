#!/usr/bin/env bash
# Pipeline batch de Spark. Cada paso deja una marca en meta y solo se repite si falta
# o si aparece en FORCE_STEPS (lista separada por comas, o "all").
#   Entrega 1: life, sentiment, closure, balance
#   Entrega 2: survival, risk
set -euo pipefail
cd /app
STEPS="${STEPS:-life sentiment closure balance survival risk report}"
for step in $STEPS; do
  echo "================ paso: $step ================"
  bin/spark-run.sh -m jobs.run_step "$step"
done
echo "Pipeline batch terminado."
