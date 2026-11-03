"""Geohash sin dependencias externas (codificación, caja y polígono GeoJSON)."""
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_DECODE = {c: i for i, c in enumerate(_BASE32)}


def encode(lat, lon, precision=5):
    if lat is None or lon is None:
        return None
    lat_rng, lon_rng = [-90.0, 90.0], [-180.0, 180.0]
    out, bit, ch, even = [], 0, 0, True
    while len(out) < precision:
        rng, val = (lon_rng, lon) if even else (lat_rng, lat)
        mid = (rng[0] + rng[1]) / 2
        if val >= mid:
            ch = (ch << 1) | 1
            rng[0] = mid
        else:
            ch <<= 1
            rng[1] = mid
        even = not even
        bit += 1
        if bit == 5:
            out.append(_BASE32[ch])
            bit, ch = 0, 0
    return "".join(out)


def bbox(gh):
    """(lat_min, lat_max, lon_min, lon_max) de una celda."""
    lat_rng, lon_rng = [-90.0, 90.0], [-180.0, 180.0]
    even = True
    for c in gh:
        cd = _DECODE[c]
        for mask in (16, 8, 4, 2, 1):
            rng = lon_rng if even else lat_rng
            mid = (rng[0] + rng[1]) / 2
            if cd & mask:
                rng[0] = mid
            else:
                rng[1] = mid
            even = not even
    return lat_rng[0], lat_rng[1], lon_rng[0], lon_rng[1]


def center(gh):
    a, b, c, d = bbox(gh)
    return (a + b) / 2, (c + d) / 2


def polygon(gh):
    """Celda como Polygon GeoJSON (anillo cerrado, orden lon/lat)."""
    lat_min, lat_max, lon_min, lon_max = bbox(gh)
    ring = [[lon_min, lat_min], [lon_max, lat_min], [lon_max, lat_max],
            [lon_min, lat_max], [lon_min, lat_min]]
    return {"type": "Polygon", "coordinates": [ring]}


def point(lat, lon):
    return {"type": "Point", "coordinates": [float(lon), float(lat)]}


def valid_coords(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    return lat == lat and lon == lon and -90 <= lat <= 90 and -180 <= lon <= 180


def grid_zones(lat_min, lat_max, lon_min, lon_max, rows=3, cols=3):
    """Zonas por defecto: rejilla rows x cols sobre la extensión de los negocios."""
    zones = []
    dlat, dlon = (lat_max - lat_min) / rows, (lon_max - lon_min) / cols
    names_r = ["Sur", "Centro", "Norte"] if rows == 3 else [f"F{i+1}" for i in range(rows)]
    names_c = ["Oeste", "Centro", "Este"] if cols == 3 else [f"C{j+1}" for j in range(cols)]
    for i in range(rows):
        for j in range(cols):
            a, b = lat_min + i * dlat, lat_min + (i + 1) * dlat
            c, d = lon_min + j * dlon, lon_min + (j + 1) * dlon
            ring = [[c, a], [d, a], [d, b], [c, b], [c, a]]
            name = f"{names_r[i]}-{names_c[j]}" if rows == cols == 3 else f"Z{i+1}{j+1}"
            zones.append({"_id": name, "name": name,
                          "geometry": {"type": "Polygon", "coordinates": [ring]}})
    return zones
