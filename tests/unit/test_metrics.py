"""Pruebas de las fórmulas. Jenkins no despliega si alguna falla."""
import pytest

from lifemap.metrics import business_life, closure_rate, risk_label, sentiment_label


# ------------------------------------------------------------- tasa de cierre anualizada
def test_closure_rate_formula():
    # 10 cierres en 3.650 días-negocio = 10 años-negocio -> 100 % anual
    assert closure_rate(10, 3650) == pytest.approx(100.0)
    # 5 cierres en 36.500 días-negocio = 100 años-negocio -> 5 % anual
    assert closure_rate(5, 36500) == pytest.approx(5.0)
    # Igual a la AFR de discos: fallas / (días-disco / 365) * 100
    assert closure_rate(3, 1000) == pytest.approx(3 / (1000 / 365) * 100)


def test_closure_rate_is_not_a_simple_proportion():
    # 1 cierre entre 2 negocios que vivieron 730 días en total -> 50 % anual, no 50 % de cerrados
    assert closure_rate(1, 730) == pytest.approx(50.0)
    assert closure_rate(1, 365 * 4) == pytest.approx(25.0)


def test_closure_rate_edge_cases():
    assert closure_rate(0, 1000) == 0.0
    assert closure_rate(0, 0) is None
    with pytest.raises(ValueError):
        closure_rate(-1, 100)


# ------------------------------------------------------------- etiqueta de sentimiento
@pytest.mark.parametrize("stars,label", [(1, 0), (2, 0), (3, 1), (4, 2), (5, 2), (4.0, 2)])
def test_sentiment_label(stars, label):
    assert sentiment_label(stars) == label


def test_sentiment_label_invalid():
    with pytest.raises(ValueError):
        sentiment_label(6)


# ------------------------------------------------------------- vida de un negocio
def test_life_closed_business():
    life = business_life("2015-01-01", "2016-01-01", is_open=0, n_reviews=12, obs_start="2010-01-01")
    assert life["business_days"] == 366
    assert life["event"] == 1 and life["censored"] == 0
    assert life["late_entry"] is False and life["entry_days"] == 0


def test_life_open_business_is_censored():
    life = business_life("2018-05-10", "2021-12-31", is_open=1, n_reviews=40, obs_start="2010-01-01")
    assert life["event"] == 0 and life["censored"] == 1


def test_life_single_review_counts_one_day():
    life = business_life("2019-03-03", "2019-03-03", is_open=0, n_reviews=1, obs_start="2010-01-01")
    assert life["business_days"] == 1


def test_life_late_entry():
    life = business_life("2008-01-01", "2012-01-01", is_open=0, n_reviews=5, obs_start="2010-01-01")
    assert life["late_entry"] is True
    assert life["entry_days"] == 731  # 2008-01-01 -> 2010-01-01
    assert life["in_window"] is True


def test_life_died_before_window_is_excluded():
    life = business_life("2006-01-01", "2008-06-01", is_open=0, n_reviews=3, obs_start="2010-01-01")
    assert life["in_window"] is False


def test_life_rejects_inverted_dates():
    with pytest.raises(ValueError):
        business_life("2020-01-02", "2020-01-01", 1, 1, "2010-01-01")


# ------------------------------------------------------------- etiqueta del modelo de riesgo
def test_risk_label():
    t = "2020-06-30"
    assert risk_label(0, "2021-01-15", t) == 1      # cerró dentro de los 12 meses
    assert risk_label(0, "2021-09-01", t) == 0      # cerró, pero activo después del plazo
    assert risk_label(1, "2020-08-01", t) == 0      # sigue abierto
    assert risk_label(0, "2020-05-01", t) is None   # sin actividad después de T: se excluye
    assert risk_label(1, "2020-06-30", t) is None
