# Informe técnico · Entrega 1 (máximo 6 páginas)

**LifeMap: plataforma batch, sentimiento y tasas de cierre** · Julián · Andrés · Big Data 2026-2
Área: Philadelphia · Familia: Restaurants · URL: `https://<SU-DOMINIO>.ngrok-free.app` · Commit: `<hash>`

> Plantilla: los números salen de `GET /reports/markdown` (o `/data/reports/resultados.md`). Reemplazar cada `[ ]` con resultados propios y borrar estas notas. Presupuesto de páginas sugerido entre paréntesis.

## 1. Arquitectura y despliegue (1 p.)
- Diagrama del README (servicios del Compose y flujo de datos).
- Servidor: equipo `[ ]` (RAM, CPU, SO), expuesto con ngrok en `[dominio]`. Por qué pollSCM y no webhook.
- Pipeline de Jenkins por etapas; captura del build exitoso y del build fallido que no desplegó.
- `/health`: captura con todos los servicios en `ok` y el hash igual al último commit de `main`.

## 2. Ingesta y limpieza con Dask (1 p.)
- Lectura particionada: `[n]` particiones de 64 MB para `review.json`.
- **Tabla de limpieza** (copiar de `/reports/markdown`): regla, antes, eliminados, después, justificación.
- Distribución de la etiqueta de sentimiento: negativo `[ ]` %, neutral `[ ]` %, positivo `[ ]` %.

## 3. Modelo de datos y vida de los negocios (1 p.)
- Documento GeoJSON de un negocio (ejemplo real, recortado) e índice 2dsphere (`db.businesses.getIndexes()`).
- Tabla de vida: negocios, cierres, censurados, con entrada tardía, días-negocio, mediana.
- **Limitación de la aproximación** (ver `docs/decisiones.md`): primera reseña ≠ apertura, última ≠ cierre, `is_open` como fotografía.

## 4. Modelo de sentimiento (1 p.)
- Pipeline reutilizado de SentimentStream y qué cambió (texto real, negaciones, partición por negocio).
- Tabla precision / recall / F1 por clase para Naive Bayes y regresión logística; matrices de confusión.
- **Interpretación del desbalance:** `[ ]` (por ejemplo: la clase neutral tiene el F1 más bajo porque 3 estrellas mezcla elogios y quejas; accuracy engaña con `[ ]` % de positivas).
- Modelo elegido y versión guardada (`/models/sentiment/current.json`).

## 5. MapReduce y tasas de cierre (1 p.)
- Map `(clave, (cierre, días_negocio))` → `reduceByKey`; validación contra `groupBy`: `[n]` claves idénticas.
- Fórmula anualizada y mínimo de días-negocio justificado (`docs/decisiones.md`).
- Top de tasas por categoría y por celda con IC 95 %; global `[ ]` % anual.
- Mapa o tabla del % de reseñas negativas por celda y relación con la tasa de cierre `[ ]`.

## 6. Balanceo de carga (1 p.)
- **Tabla de tiempos:** 2, 8, 32 y 200 particiones × 3 repeticiones, media y desviación, con y sin clave.
- Filas por partición (`spark_partition_id()`): sin clave vs ciudad vs categoría; máx/media.
- Interpretación: `[ ]` (por ejemplo: con clave `city`, Philadelphia concentra `[ ]` % de las filas en una partición; con 200 particiones crece el sobrecosto de tareas pequeñas).

## Aportes
| Integrante | Ramas / PR |
|---|---|
| Julián | `[ ]` |
| Andrés | `[ ]` |
