import pytest

from lifemap import features, geo


def test_geohash_known_value():
    assert geo.encode(42.6, -5.6, 5) == "ezs42"
    assert geo.encode(57.64911, 10.40744, 11) == "u4pruydqqvj"


def test_geohash_polygon_contains_point():
    lat, lon = 39.9526, -75.1652
    gh = geo.encode(lat, lon, 6)
    lat_min, lat_max, lon_min, lon_max = geo.bbox(gh)
    assert lat_min <= lat <= lat_max and lon_min <= lon <= lon_max
    ring = geo.polygon(gh)["coordinates"][0]
    assert ring[0] == ring[-1] and len(ring) == 5


def test_valid_coords():
    assert geo.valid_coords(39.9, -75.1)
    assert not geo.valid_coords(None, 1)
    assert not geo.valid_coords(95, 0)
    assert not geo.valid_coords(float("nan"), 0)


def test_grid_zones():
    zones = geo.grid_zones(0, 3, 0, 3)
    assert len(zones) == 9 and len({z["_id"] for z in zones}) == 9


def test_trend_slope_increasing():
    monthly = {"2020-01": [10, 1], "2020-02": [10, 2], "2020-03": [10, 3]}
    # +10 puntos porcentuales por mes
    assert features.trend_slope(monthly, "2020-03-31") == pytest.approx(10.0)


def test_trend_slope_ignores_old_months_and_needs_two_points():
    monthly = {"2019-01": [10, 10], "2020-03": [10, 5]}
    assert features.trend_slope(monthly, "2020-03-31") == 0.0


def test_update_and_compute_features():
    st = {"competitors_500m": 4}
    features.update_state(st, "2020-01-10", 5, 1.0, False)
    features.update_state(st, "2020-02-10", 1, -1.0, True)
    f = features.compute_features(st, "2020-03-10")
    assert f["sentiment_mean"] == 0.0
    assert f["stars_mean"] == 3.0
    assert f["days_since_last"] == 29
    assert f["competitors_500m"] == 4.0
    assert set(f) == set(features.FEATURE_NAMES)
    assert st["monthly"] == {"2020-01": [1, 0], "2020-02": [1, 1]}
