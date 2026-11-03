"""Fórmulas del proyecto. Funciones puras: se prueban en Jenkins (tests/unit)."""
from datetime import date, datetime


def closure_rate(closures, business_days):
    """Tasa de cierre anualizada (%) = cierres / (días-negocio / 365) * 100.

    Es la AFR del ejercicio de discos con días-negocio en lugar de días-disco.
    Devuelve None si no hay exposición (no se divide entre cero).
    """
    if closures < 0 or business_days < 0:
        raise ValueError("cierres y días-negocio deben ser no negativos")
    if business_days == 0:
        return None
    return closures / business_days * 100.0


def sentiment_label(stars):
    """1-2 estrellas = 0 (negativo), 3 = 1 (neutral), 4-5 = 2 (positivo)."""
    stars = int(round(float(stars)))
    if stars not in (1, 2, 3, 4, 5):
        raise ValueError(f"estrellas fuera de rango: {stars}")
    if stars <= 2:
        return 0
    if stars == 3:
        return 1
    return 2


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)[:19]).date()


def business_life(first_review, last_review, is_open, n_reviews, obs_start):
    """Vida aproximada de un negocio (primera a última reseña), evento, censura y entrada tardía.

    - business_days = (última - primera) + 1 día: un negocio con una sola reseña aporta 1 día.
    - event = 1 si is_open == 0 (cierre observado); censored = 1 si sigue abierto.
    - Entrada tardía: si la primera reseña es anterior al inicio de la ventana observada
      (obs_start), el negocio entra al estudio con edad entry_days > 0, igual que un disco
      que ya venía funcionando. Si además murió antes de obs_start, nunca estuvo "en riesgo"
      dentro de la ventana (in_window = False) y se excluye de Kaplan-Meier.
    """
    first, last, obs = _as_date(first_review), _as_date(last_review), _as_date(obs_start)
    if last < first:
        raise ValueError("la última reseña no puede ser anterior a la primera")
    days = (last - first).days + 1
    event = 1 if int(is_open) == 0 else 0
    entry = max(0, (obs - first).days)
    return {
        "first_review": first.isoformat(),
        "last_review": last.isoformat(),
        "n_reviews": int(n_reviews),
        "business_days": days,
        "event": event,
        "censored": 1 - event,
        "late_entry": entry > 0,
        "entry_days": min(entry, days),
        "in_window": last >= obs,
    }


def risk_label(is_open, last_review, cutoff, horizon_days=365):
    """Etiqueta del modelo de riesgo (Entrega 2).

    1 -> cerrado y su última reseña cae en (T, T + 12 meses].
    0 -> sigue con actividad después de T + 12 meses o sigue abierto.
    None -> sin actividad después de T: se excluye.
    """
    last, t = _as_date(last_review), _as_date(cutoff)
    if last <= t:
        return None
    horizon = date.fromordinal(t.toordinal() + horizon_days)
    if int(is_open) == 0 and last <= horizon:
        return 1
    return 0
