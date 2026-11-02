# Informe técnico final · Entrega 2 (máximo 10 páginas)

**LifeMap: streaming, supervivencia y alerta temprana de cierre** · Julián · Andrés · Big Data 2026-2

> Plantilla: reutilizar las secciones 1–6 de la Entrega 1 resumidas (máximo 4 páginas) y agregar lo siguiente con resultados propios.

## 7. Arquitectura completa y decisiones de streaming (1,5 p.)
- Kafka KRaft, topic `reviews` con 3 particiones y clave `business_id` (las reseñas de un negocio quedan ordenadas en una partición).
- Productor: T = `2020-06-30`, solo reseñas posteriores, orden por fecha, velocidad `--days-per-tick` / `--tick-seconds`.
- Streaming: carga del modelo de la Entrega 1 sin reentrenar (`stream_status` en MongoDB), dos consultas (`reviews` con `foreachBatch`, `windows` con watermark).
- Ventanas de 30 días sobre `date` con watermark de 7 días; regla de hotspot y su justificación.
- `/health`: captura con Kafka y el retraso del consumidor durante la demo.

## 8. Supervivencia (2 p.)
- Curva global con y sin corrección de entrada tardía: diferencia en la mediana `[ ]` (sesgo de ignorar el truncamiento).
- Curvas por categoría principal y por zona (`$geoWithin`), con número en riesgo y bandas de confianza.
- Mediana de vida cuando existe; grupos excluidos por tamaño (n < 30 o < 10 eventos).
- Interpretación `[ ]`.

## 9. Modelo de riesgo de cierre (2 p.)
- Variables (solo ≤ T) y por qué se excluyen `stars`, `review_count` e `is_open` del archivo de negocios.
- Etiqueta, negocios excluidos, prevalencia `[ ]` %.
- Tabla: AUC-ROC, AUC-PR (frente a la prevalencia), precision, recall y F1 en el umbral, para LR y GBT.
- Umbral por F2 en validación; interpretación frente al desbalance `[ ]`.

## 10. Alertas y dashboard (1 p.)
- Cómo un micro-lote actualiza el estado del negocio, recalcula variables y puntúa (`foreachBatch`).
- Capturas del dashboard de Power BI (cuatro visualizaciones) en dos momentos de la demo.

## 11. Reflexión: qué se reutilizó (0,5 p.)
| Origen | Reutilizado | Qué cambió |
|---|---|---|
| SentimentStream | Pipeline TF-IDF + NB, `/predict` | `[ ]` |
| KafkaMed | Productor, consumidor con inferencia, alerta temprana, Power BI | `[ ]` |
| Trabajo geoespacial | Compose, Kaggle API, Dask, MongoDB 2dsphere, Flask, Jenkins | `[ ]` |
| Ejercicio de discos | `reduceByKey`, AFR, balanceo, watermark, censura, KM | `[ ]` |

## Limitaciones
Vida aproximada por reseñas, `is_open` como fotografía, T en pandemia, ventanas alineadas a la época Unix, zonas como rejilla (o polígonos propios en `config/zones.geojson`).
