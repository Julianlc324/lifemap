"""Ejecuta un paso del pipeline batch si no está hecho (o si está en FORCE_STEPS)."""
import importlib
import importlib.util
import sys
import time

from lifemap import steps

STEPS = {
    "life": "jobs.business_life",          # E1.5 vida de cada negocio
    "sentiment": "jobs.sentiment_train",   # E1.6 Naive Bayes vs regresión logística
    "closure": "jobs.closure_rates",       # E1.7 MapReduce y tasa de cierre
    "balance": "jobs.load_balancing",      # E1.8 balanceo de carga
    "survival": "jobs.survival",           # E2.5 Kaplan-Meier
    "risk": "jobs.risk_train",             # E2.6 modelo de riesgo de cierre
    "report": "jobs.report",               # tablas para el informe (siempre se regenera)
}
ALWAYS = {"report"}


def main(name):
    if name not in STEPS:
        raise SystemExit(f"Paso desconocido: {name}. Opciones: {', '.join(STEPS)}")
    if importlib.util.find_spec(STEPS[name]) is None:
        print(f"[{name}] {STEPS[name]} aún no está en el repositorio (Entrega 2); se omite")
        return
    steps.wait_for_mongo()
    if name not in ALWAYS and not steps.should_run(name):
        return
    t0 = time.time()
    info = importlib.import_module(STEPS[name]).run() or {}
    if name not in ALWAYS:
        steps.mark_done(name, seconds=round(time.time() - t0, 1), **info)
    print(f"[{name}] terminado en {time.time() - t0:.0f} s")


if __name__ == "__main__":
    main(sys.argv[1])
