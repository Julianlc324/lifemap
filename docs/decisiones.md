# Decisiones y justificaciones

Todas se configuran en `config/lifemap.env`; cambiarlas no requiere tocar código.

## Alcance
- **Área:** Philadelphia, por caja de coordenadas `METRO_BBOX = 39.70,40.30,-75.60,-74.90` (incluye los suburbios de Pensilvania y Nueva Jersey). Se usa una caja y no el campo `city` porque en Yelp la ciudad está escrita de muchas formas ("Philly", "Phila", barrios).
- **Familia:** `Restaurants`: el negocio debe tener esa categoría en su lista.
- **Categoría principal:** la primera de la lista que no sea genérica (Restaurants, Food, Nightlife, Bars, ...). Si todas son genéricas, se usa la familia.

## Limpieza (Dask)
Las reglas se aplican en orden y el reporte (`/reports/markdown`, colección `cleaning_report`) muestra antes, eliminados y después de cada una:

| Regla | Justificación |
|---|---|
| Coordenadas nulas | Sin coordenadas no hay GeoJSON ni consultas geoespaciales. |
| Coordenadas fuera de rango | Romperían el índice 2dsphere. |
| Fuera del área / de la familia | Alcance asignado al equipo. |
| Sin categorías | No se puede asignar categoría principal. |
| `is_open` inválido | Define evento y censura. |
| `business_id` / `review_id` duplicado | Contaría dos veces días-negocio, cierres o reseñas. |
| Reseña de negocio fuera de alcance | Unión reseña–negocio (inner join). |
| Reseña sin texto | No hay entrada para TF-IDF. |
| Estrellas fuera de 1–5 o fecha inválida | Son la etiqueta y el eje temporal. |

## Vida de un negocio
- `business_days = última reseña − primera reseña + 1` (un negocio con una sola reseña aporta 1 día).
- **Limitación:** Yelp no trae fechas de apertura ni cierre. La primera reseña ocurre después de la apertura (subestima la vida) y la última reseña ocurre antes del cierre real (el cierre se fecha antes). Los negocios con pocas reseñas tienen vidas muy subestimadas. Además, `is_open` es una fotografía en la fecha de corte del dataset: un negocio cerrado hace años y uno cerrado la semana anterior al corte cuentan igual.
- **Censura:** `is_open = 1` → censurado en la fecha de corte del dataset (última reseña presente).
- **Entrada tardía:** la ventana observada empieza en `OBS_START = 2010-01-01`. Antes de 2010 la cobertura de Yelp era escasa: la primera reseña de un negocio de esa época no indica su apertura. Un negocio con primera reseña anterior entra al conjunto en riesgo con edad `entry_days = OBS_START − primera reseña`; los que murieron antes de `OBS_START` no se cuentan en Kaplan-Meier (nunca estuvieron en riesgo en la ventana). Es el mismo tratamiento de los discos que ya venían funcionando.

## Tasa de cierre anualizada y mínimo de días-negocio
`tasa (%) = cierres / (días-negocio / 365) × 100`.

**Mínimo: 18.250 días-negocio (50 años-negocio).** Con tasas típicas de 3–8 % anual, 50 años-negocio dan apenas entre 1,5 y 4 cierres esperados, así que por debajo de ese tamaño la tasa depende de uno o dos cierres. Por eso, además del mínimo, cada tasa lleva su IC 95 % exacto de Poisson (Garwood): con 50 años-negocio y 3 cierres (6 %), el IC va aproximadamente de 1,2 % a 17,5 %. Es el tamaño más pequeño en que la tasa empieza a ser informativa. Si el equipo quiere comparar con más seguridad, se sube `MIN_BUSINESS_DAYS` (el endpoint `/stats/closure` lo acepta como parámetro `min_days`). *Revisar con los datos reales:* en el informe reportar cuántos grupos quedan por encima del mínimo y ajustar si son muy pocos.

## Sentimiento
- Etiqueta: 1–2 estrellas negativo, 3 neutral, 4–5 positivo (clases desbalanceadas; se reporta la distribución).
- **Stopwords:** lista en inglés de Spark **sin negaciones** ("not", "no", "never", "don"...): quitarlas convierte "not good" en "good".
- **Partición por negocio:** `pmod(xxhash64(business_id), 100) < 80` → entrenamiento. Determinística y sin fuga; el job verifica que ningún negocio quede en ambos lados.
- Se elige el modelo con mayor **F1 macro** (no accuracy): con ~70 % positivas, un modelo que siempre dice "positivo" tendría accuracy alto y sería inútil para detectar reseñas negativas.

## MapReduce y balanceo
- Claves: `("category", categoría principal)` y `("cell", geohash de precisión 5)` (~4,9 × 4,9 km).
- Validación: el resultado de `reduceByKey` se compara clave por clave con `groupBy().agg(sum)`; el job falla si difieren.
- Balanceo: se mide el MapReduce del % de reseñas negativas (una fila por reseña) porque el de tasas (una fila por negocio) es tan pequeño que el tiempo es casi todo sobrecosto. La entrada se cachea antes de medir y se apaga AQE para que Spark no fusione particiones.

## Entrega 2
- **T = 2020-06-30.** Deja 12 meses completos para la etiqueta (hasta 2021-06-30) y ~19 meses de reseñas posteriores para el streaming (hasta enero de 2022). *Limitación:* T cae en la pandemia; los cierres de 2020–2021 tienen causas externas al sentimiento.
- **Ventanas:** 30 días sobre `date` (tiempo del evento) con watermark de 7 días; ventanas alineadas a la época Unix (comportamiento de `window()` de Spark).
- **Hotspot:** el % negativo sube **≥ 10 puntos porcentuales** frente a la ventana anterior de la misma celda, con **≥ 15 reseñas** en ambas. Con 15 reseñas, una sola reseña negativa extra mueve el % en ~6,7 pp; exigir 10 pp evita que una reseña aislada encienda la alarma.
- **Modelo de riesgo:** no se usan `stars`, `review_count` ni `is_open` del archivo de negocios como variables porque son una fotografía al final del dataset (miran después de T). Se usan estrellas y conteos calculados solo con reseñas ≤ T. Competidores: misma categoría, a 500 m (`$geoNear`), con actividad en los 12 meses previos a T.
- **Umbral:** se elige en validación maximizando **F2** (pesa el recall el doble que la precisión): en alerta temprana es peor no avisar de un cierre que revisar una falsa alarma. Se reporta AUC-PR frente a la prevalencia, que es el AUC-PR de un modelo al azar.
