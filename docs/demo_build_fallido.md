# Demostrar que una prueba fallida detiene el despliegue (E1.3 y E2.9)

Hacerlo una vez por entrega, antes de la sustentación, para que quede el build fallido en el historial de Jenkins.

## Entrega 1: fórmula de la tasa de cierre

```bash
git switch -c demo/build-fallido
# Romper la fórmula: quitar la anualización
sed -i 's#return closures / (business_days / 365.0) \* 100.0#return closures / business_days * 100.0#' lifemap/metrics.py
git commit -am "Demo: fórmula sin anualizar (debe fallar en Jenkins)"
git push -u origin demo/build-fallido
```
Abrir PR y hacer merge a `main`. En Jenkins el build falla en **Unit tests** (`test_closure_rate_formula`) y no llega a **Deploy**: `/health` sigue mostrando el commit anterior. Tomar captura del build en rojo y de `/health`.

Revertir: en GitHub, botón **Revert** del PR → merge. Jenkins vuelve a desplegar (build en verde).

## Entrega 2: prueba de humo del streaming

Detener el consumidor antes de un build: `docker compose stop streaming` no sirve (Jenkins lo vuelve a levantar), así que se rompe el código:

```python
# streaming/stream_job.py, en process_reviews: escribir en otra colección
database.reviews_stream_X.bulk_write(ops, ordered=False)
```
El build falla en **Streaming tests** (`solo 0/100 mensajes clasificados`) y la API pública no se reemplaza. Revertir igual que arriba.
