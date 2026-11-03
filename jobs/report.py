"""Genera tablas en Markdown con los resultados propios para pegar en el informe.

Salida: /data/reports/resultados.md (también disponible en la API: /reports/markdown).
"""
from pathlib import Path

from lifemap import config, db


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def fmt(n):
    return f"{n:,.0f}".replace(",", ".") if isinstance(n, (int, float)) else str(n)


def build():
    database = db.get_db()
    parts = [f"# Resultados LifeMap - {config.METRO_NAME} / {config.CATEGORY_FAMILY}\n"]
    ingest = db.get_meta("step:ingest")
    dataset = db.get_meta("dataset")

    rep = list(database.cleaning_report.find().sort("orden", 1))
    if rep:
        parts.append("## Limpieza con Dask (conteos antes y después de cada regla)\n")
        parts.append(table(["Archivo", "Regla", "Antes", "Eliminados", "Después", "Justificación"],
                           [[r["dataset"], r["regla"], fmt(r["antes"]), fmt(r["eliminados"]),
                             fmt(r["despues"]), r["justificacion"]] for r in rep]))
        if ingest.get("sentiment_distribution"):
            parts.append("\nDistribución de la etiqueta de sentimiento (todas las reseñas): " +
                         ", ".join(f"{k}: {fmt(v)}" for k, v in ingest["sentiment_distribution"].items()))

    if dataset.get("life_stats"):
        s = dataset["life_stats"]
        parts.append("\n## Vida de los negocios\n")
        parts.append(table(["Negocios", "Cierres (evento)", "Censurados", "Entrada tardía",
                            "Días-negocio", "Mediana días", "Corte del dataset"],
                           [[fmt(s["negocios"]), fmt(s["cierres"]), fmt(s["censurados"]),
                             fmt(s["entrada_tardia"]), fmt(s["dias_negocio"]), fmt(s["mediana_dias"]),
                             str(dataset.get("cutoff_date"))[:10]]]))

    sm = list(database.model_metrics.find({"model": "sentiment"}))
    if sm:
        parts.append("\n## Modelo de sentimiento (prueba, partición por negocio)\n")
        rows = []
        for m in sm:
            for cls, v in m["per_class"].items():
                rows.append([m["classifier"] + (" (elegido)" if m.get("chosen") else ""), cls,
                             v["precision"], v["recall"], v["f1"], fmt(v["support"])])
        parts.append(table(["Clasificador", "Clase", "Precision", "Recall", "F1", "Soporte"], rows))
        for m in sm:
            parts.append(f"\n**{m['classifier']}**: accuracy {m['accuracy']}, F1 macro {m['macro_f1']}. "
                         f"Matriz de confusión (filas = real {m['confusion_labels']}, columnas = predicho):\n")
            parts.append(table(["real \\ pred"] + m["confusion_labels"],
                               [[lbl] + row for lbl, row in zip(m["confusion_labels"], m["confusion_matrix"])]))

    for by, title in (("category", "categoría principal"), ("cell", "celda geohash")):
        rates = list(database.closure_rates.find({"by": by, "sufficient": True})
                     .sort("closure_rate_pct", -1).limit(10))
        if rates:
            parts.append(f"\n## Tasa de cierre anualizada por {title} (top 10, >= {fmt(config.MIN_BUSINESS_DAYS)} días-negocio)\n")
            parts.append(table(["Clave", "Negocios", "Cierres", "Años-negocio", "Tasa %", "IC 95 %"],
                               [[r["key"], r["n_businesses"], r["closures"], r["business_years"],
                                 r["closure_rate_pct"], f"{r['ci95_low']} – {r['ci95_high']}"] for r in rates]))

    tim = list(database.load_balancing.find({"kind": "timing"}).sort([("strategy", 1), ("partitions", 1)]))
    if tim:
        parts.append("\n## Balanceo de carga: tiempos (3 repeticiones)\n")
        parts.append(table(["Estrategia", "Particiones", "Tiempos (s)", "Media (s)", "Desv. est. (s)"],
                           [[t["strategy"], t["partitions"], ", ".join(map(str, t["times_s"])),
                             t["mean_s"], t["std_s"]] for t in tim]))
        skew = list(database.load_balancing.find({"kind": "skew"}).sort([("partitions", 1), ("key", 1)]))
        parts.append("\n## Balanceo de carga: filas por partición (spark_partition_id)\n")
        parts.append(table(["Particiones", "Clave", "No vacías", "Mín", "Máx", "Media", "Desv.", "Máx/Media"],
                           [[s["partitions"], s["key"], s["non_empty"], fmt(s["min"]), fmt(s["max"]),
                             fmt(s["mean"]), fmt(s["std"]), s["skew_max_over_mean"]] for s in skew]))

    surv = list(database.survival.find({}, {"curve": 0, "geometry": 0}).sort([("group_type", 1), ("n", -1)]))
    if surv:
        parts.append("\n## Supervivencia (Kaplan-Meier con censura y entrada tardía)\n")
        parts.append(table(["Tipo", "Grupo", "n", "Eventos", "Mediana (años)", "S(1 año)", "S(5 años)", "Suficiente"],
                           [[s["group_type"], s["group"], s["n"], s["events"], s.get("median_years") or "no se alcanza",
                             s.get("survival_1y"), s.get("survival_5y"), "sí" if s["sufficient"] else "no"]
                            for s in surv[:40]]))

    rk = list(database.model_metrics.find({"model": "risk"}))
    if rk:
        parts.append(f"\n## Modelo de riesgo de cierre (T = {config.CUTOFF_T}, conjunto de prueba)\n")
        parts.append(table(["Clasificador", "AUC-ROC", "AUC-PR", "Prevalencia (AUC-PR azar)", "Umbral",
                            "Precision", "Recall", "F1"],
                           [[r["classifier"] + (" (elegido)" if r.get("chosen") else ""), r["test"]["auc_roc"],
                             r["test"]["auc_pr"], r["test"]["prevalence"], r["test"]["threshold"],
                             r["test"]["precision"], r["test"]["recall"], r["test"]["f1"]] for r in rk]))
    return "\n".join(parts) + "\n"


def run():
    md = build()
    out = Path(config.DATA_DIR, "reports", "resultados.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md)
    db.set_meta("report", markdown=md)
    print(md)
    return {}
