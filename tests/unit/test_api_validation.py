"""La API valida entradas antes de consultar MongoDB (no necesita base de datos)."""
import json

import pytest

from api.app import app
from api.validation import ApiError, polygon


@pytest.fixture()
def client():
    app.config["TESTING"] = True
    return app.test_client()


@pytest.mark.parametrize("url", [
    "/businesses/near?lat=abc&lon=-75",
    "/businesses/near?lat=95&lon=-75",
    "/businesses/near?lon=-75",
    "/businesses/near?lat=39.9&lon=-75&radius=-5",
    "/zones/summary?lat=39.9&lon=-500",
    "/stats/closure?by=pais",
    "/stats/closure?min_days=-1",
    "/businesses/within?polygon=nojson",
    "/live/hotspots?lat=39.9&lon=-75&window=ayer",
    "/risk/near?lat=39.9",
    "/predict",
])
def test_invalid_inputs_return_400(client, url):
    r = client.get(url)
    assert r.status_code == 400, r.get_json()
    assert "error" in r.get_json()


def test_ping(client):
    r = client.get("/ping")
    assert r.status_code == 200 and r.get_json()["status"] == "ok"


def test_polygon_validation():
    ok = {"type": "Polygon", "coordinates": [[[-75.2, 39.9], [-75.1, 39.9], [-75.1, 40.0], [-75.2, 39.9]]]}
    assert polygon(json.dumps(ok))["type"] == "Polygon"
    with pytest.raises(ApiError):
        polygon({"type": "Polygon", "coordinates": [[[-75.2, 39.9], [-75.1, 39.9], [-75.1, 40.0]]]})
    with pytest.raises(ApiError):  # [lat, lon] invertido fuera de rango
        polygon({"type": "Polygon", "coordinates": [[[39.9, -175.2], [39.9, -75.1], [40, -75.1], [39.9, -175.2]]]})
