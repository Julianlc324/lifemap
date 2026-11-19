"""Escrituras repetibles: reconstruir desde la base T y reseñas únicas persistidas."""
from copy import deepcopy

from lifemap import config, features


def rebuild_state(baseline, reviews):
    state = deepcopy(baseline)
    for review in reviews:
        features.update_state(state, review['date'], review['stars'],
                              config.SENTIMENT_SCORE[review['prediction_id']],
                              review['prediction_id'] == 0)
    return state



def hotspot_fields(window, previous):
    prev = previous if previous and previous['window_end'] == window['window_start'] else None
    delta = window['pct_negative'] - prev['pct_negative'] if prev else None
    return {
        'prev_pct_negative': prev['pct_negative'] if prev else None,
        'prev_count': prev['count'] if prev else None,
        'delta_pp': round(delta, 4) if delta is not None else None,
        'hotspot': bool(prev and window['count'] >= config.HOTSPOT_MIN_REVIEWS
                        and prev['count'] >= config.HOTSPOT_MIN_REVIEWS
                        and delta >= config.HOTSPOT_DELTA_PP)}

def refresh_hotspots(database, geohashes):
    """Guardar primero todos los conteos, luego reparar comparaciones y sucesoras.

    También corrige una sucesora cuando llega una revisión tardía a su predecesora.
    """
    for gh in sorted(set(geohashes)):
        previous = None
        for window in database.windows.find({'geohash': gh}).sort('window_start', 1):
            database.windows.update_one({'_id': window['_id']}, {
                '$set': hotspot_fields(window, previous)})
            previous = window
