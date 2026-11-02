"""Validación de parámetros de entrada de la API (respuestas 400 con mensaje claro)."""
import json
from datetime import datetime


class ApiError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.message, self.status = message, status


def number(args, name, default=None, lo=None, hi=None, required=False, cast=float):
    raw = args.get(name)
    if raw in (None, ""):
        if required:
            raise ApiError(f"Falta el parámetro '{name}'")
        return default
    try:
        value = cast(raw)
    except (TypeError, ValueError):
        raise ApiError(f"'{name}' debe ser numérico (recibido: {raw!r})") from None
    if value != value:  # NaN
        raise ApiError(f"'{name}' no puede ser NaN")
    if lo is not None and value < lo or hi is not None and value > hi:
        raise ApiError(f"'{name}' debe estar entre {lo} y {hi} (recibido: {value})")
    return value


def lat_lon(args, required=True):
    lat = number(args, "lat", lo=-90, hi=90, required=required)
    lon = number(args, "lon", lo=-180, hi=180, required=required)
    return lat, lon


def text(args, name, max_len=200, required=False):
    raw = args.get(name)
    if raw in (None, ""):
        if required:
            raise ApiError(f"Falta el parámetro '{name}'")
        return None
    raw = str(raw).strip()
    if len(raw) > max_len:
        raise ApiError(f"'{name}' supera {max_len} caracteres")
    return raw


def date_param(args, name):
    raw = args.get(name)
    if raw in (None, "", "latest"):
        return None
    try:
        return datetime.fromisoformat(raw[:10])
    except ValueError:
        raise ApiError(f"'{name}' debe ser una fecha AAAA-MM-DD o 'latest'") from None


def polygon(raw):
    """Acepta un Polygon GeoJSON (dict o cadena JSON) o un Feature con geometría Polygon."""
    if raw in (None, ""):
        raise ApiError("Falta el polígono GeoJSON ('polygon')")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raise ApiError("'polygon' no es JSON válido") from None
    if isinstance(raw, dict) and raw.get("type") == "Feature":
        raw = raw.get("geometry")
    if not isinstance(raw, dict) or raw.get("type") != "Polygon":
        raise ApiError("'polygon' debe ser una geometría GeoJSON de tipo Polygon")
    rings = raw.get("coordinates")
    if not isinstance(rings, list) or not rings:
        raise ApiError("El polígono no tiene coordenadas")
    for ring in rings:
        if not isinstance(ring, list) or len(ring) < 4:
            raise ApiError("Cada anillo necesita al menos 4 posiciones")
        if ring[0] != ring[-1]:
            raise ApiError("El anillo debe cerrarse (primera posición = última)")
        for pos in ring:
            if (not isinstance(pos, (list, tuple)) or len(pos) < 2
                    or not all(isinstance(c, (int, float)) for c in pos[:2])):
                raise ApiError("Las posiciones deben ser [lon, lat] numéricas")
            if not (-180 <= pos[0] <= 180 and -90 <= pos[1] <= 90):
                raise ApiError(f"Posición fuera de rango: {pos} (orden GeoJSON: [lon, lat])")
    return {"type": "Polygon", "coordinates": rings}
