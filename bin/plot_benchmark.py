"""Regenera la figura del benchmark distribuido a partir de su evidencia JSON."""
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

root = Path(__file__).resolve().parents[1]
data = json.loads((root / "docs/evidencias/jenkins-candidate-2-2026-11-19/load_balancing.json").read_text())
fig, ax = plt.subplots(figsize=(9, 4.6), constrained_layout=True)
for key, label, color in (("sin clave", "Sin clave", "#1676ab"),
                           ("con clave: main_category", "Por categoría", "#d66d1b")):
    rows = sorted((r for r in data["timings"] if r["strategy"] == key), key=lambda r: r["partitions"])
    ax.errorbar(range(len(rows)), [r["mean_s"] for r in rows],
                yerr=[r["std_s"] for r in rows], marker="o", capsize=4, color=color, label=label)
ax.set_xticks(range(4), ["2", "8", "32", "200"])
ax.set(xlabel="Particiones", ylabel="Tiempo (s): media ± desviación estándar",
       title="LifeMap · Clúster Spark · 13.883 negocios · 3 repeticiones")
ax.grid(alpha=.2)
ax.legend(frameon=False)
out = root / "docs/figuras/benchmark_cluster.png"
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=180)
plt.close(fig)
