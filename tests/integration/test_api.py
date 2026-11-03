"""Pruebas contra la API candidata (Jenkins: API_URL=http://api-test:5000) o la URL pública.

    API_URL=https://su-dominio.ngrok-free.app pytest -q tests/integration
"""
import math
import os

import pytest
import requests

API = os.environ.get("API_URL")
pytestmark = [pytest.mark.integration,
              pytest.mark.skipif(not API, reason="API_URL no definido")]
HEADERS = {"ngrok-skip-browser-warning": "1"}


def get(path, **params):
    return requests.get(f"{API}{path}", params=params, headers=HEADERS, timeout=120)


@pytest.fixture(scope="module")
def center():
    return get("/config").json()["center"]


def haversine(lat1, lon1, lat2, lon2):
    r = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    return 2 * r * math.asin(math.sqrt(math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2))


def test_health_reports_every_service_and_commit():
    body = get("/health").json()
    assert set(body["services"]) >= {"api", "mongodb", "spark", "dask", "kafka"}
    assert body["services"]["mongodb"]["status"] == "ok"
    expected = os.environ.get("EXPECTED_COMMIT")
    if expected:
        assert body["commit"] == expected


def test_businesses_near_respects_radius(center):
    lat, lon = center
    r = get("/businesses/near", lat=lat, lon=lon, radius=3000, limit=50)
    assert r.status_code == 200
    items = r.json()["items"]
    assert items, "no hay negocios cerca del centro del área"
    for b in items:
        blon, blat = b["location"]["coordinates"]
        assert haversine(lat, lon, blat, blon) <= 3000 + 5


def test_businesses_near_with_category(center):
    near = get("/businesses/near", lat=center[0], lon=center[1], radius=5000, limit=5).json()["items"]
    cat = near[0]["main_category"]
    items = get("/businesses/near", lat=center[0], lon=center[1], radius=5000, category=cat).json()["items"]
    assert items and all(cat == b["main_category"] or cat in b.get("categories", []) for b in items)


def test_businesses_within_polygon(center):
    lat, lon = center
    d = 0.02
    ring = [[lon - d, lat - d], [lon + d, lat - d], [lon + d, lat + d], [lon - d, lat + d], [lon - d, lat - d]]
    r = requests.post(f"{API}/businesses/within", json={"polygon": {"type": "Polygon", "coordinates": [ring]}},
                      headers=HEADERS, timeout=60)
    assert r.status_code == 200
    for b in r.json()["items"]:
        blon, blat = b["location"]["coordinates"]
        assert lon - d <= blon <= lon + d and lat - d <= blat <= lat + d


def test_zones_summary(center):
    body = get("/zones/summary", lat=center[0], lon=center[1], radius=3000).json()
    assert body["businesses"] > 0
    assert body["closure_rate_pct"] == pytest.approx(body["closures"] / (body["business_days"] / 365) * 100, rel=1e-2)


@pytest.mark.parametrize("by", ["category", "cell"])
def test_stats_closure_uses_annualized_rate_and_min_days(by):
    body = get("/stats/closure", by=by, min_days=0, limit=100).json()
    assert body["items"]
    for it in body["items"]:
        expected = it["closures"] / (it["business_days"] / 365) * 100
        assert it["closure_rate_pct"] == pytest.approx(expected, abs=1e-2)
    strict = get("/stats/closure", by=by, min_days=36500).json()
    assert all(it["business_days"] >= 36500 for it in strict["items"])


def test_predict_negative_vs_positive():
    neg = get("/predict", text="Terrible service, cold food, rude staff. Never coming back.").json()
    pos = get("/predict", text="Amazing food and friendly staff, best dinner we had in years!").json()
    assert neg["label"] in ("negativo", "neutral", "positivo")
    assert neg["probabilities"]["negativo"] > pos["probabilities"]["negativo"]
    assert pos["label"] == "positivo"


def test_entrega2_endpoints_respond(center):
    lat, lon = center
    assert get("/live/hotspots", lat=lat, lon=lon, radius=20000).status_code == 200
    assert get("/alerts", lat=lat, lon=lon, radius=20000).status_code == 200
    assert get("/risk/near", lat=lat, lon=lon, radius=2000).status_code == 200
    r = get("/survival/todos")
    assert r.status_code == 200 and r.json()["n"] > 0
