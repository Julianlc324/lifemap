# Auditoría previa y plan de implementación de LifeMap

Fecha: 19 de noviembre de 2026 (America/Bogota). Base revisada: `ab0ffb2` de `origin/main`.

## Fuentes y alcance

Se leyeron conjuntamente `LIFEMAP_GUIA_PROYECTO.md` y `LIFEMAP_DIAPOSITIVAS_APOYO.md`, ubicados en la carpeta superior LifeMap. La guía es la autoridad para requisitos, entregables y evaluación; las diapositivas explican cómo implementarlos. Este documento registra el análisis anterior a la programación de E2.

Se clonó https://github.com/Julianlc324/lifemap.git en `LifeMap /lifemap`. La comparación de archivos con `lifemap-main` no encontró diferencias, excluyendo `.git`. La copia descargada original se conserva intacta. El repositorio remoto ya contiene contribuciones de Andrés y Julián, ramas de funcionalidades y PR integrados; no es necesario reconstruir ni simular ese historial.

Andrés confirmó su identidad `@Bjonion`, correo `agomezp@correo.iue.edu.co`, la disponibilidad de la VM y el alcance Philadelphia / Restaurants. También confirmó que conserva resultados de E1 fuera del repositorio; falta localizar y revisar esos archivos. No se presupone que exista un `.pbix` terminado.

## Verificación realizada

- `pytest -q tests/unit`: **37 pruebas aprobadas**, Python 3.9.6 en un entorno temporal aislado. Advertencia de urllib3 por LibreSSL del Python del equipo; no es una falla de las pruebas ni una validación de las imágenes Docker.
- `docker compose config --quiet`: correcto. Esto valida la configuración, no el arranque de servicios.
- Docker CLI está instalado, pero el daemon no responde. No se ejecutaron Spark, Dask, MongoDB ni Kafka localmente.
- Consulta a `http://lifemap-iue.mexicocentral.cloudapp.azure.com/health`: timeout tras 20 segundos. No se pudo verificar disponibilidad, servicios ni hash desplegado. El timeout no identifica por sí solo la causa.
- El historial contiene PR #14 de demostración de fórmula fallida y PR #15 de reversión. Falta consultar Jenkins para comprobar el build fallido y el mantenimiento de la versión previa.
- No se encontraron modelos, métricas propias, curvas, capturas ni archivo `.pbix` en el árbol versionado. Los informes existentes son plantillas; la existencia de resultados externos fue confirmada por Andrés.

Los estados siguientes describen evidencia de código, no una nota académica. Un criterio con código implementado sigue pendiente hasta verificar sus resultados y, cuando corresponda, la URL pública.

## Matriz de criterios: Entrega 1

| Criterio | Evidencia actual | Pendiente para aceptación |
| --- | --- | --- |
| E1.1 Orquestación | Compose con MongoDB, Spark master y dos workers, Dask scheduler y dos workers, Flask, Jenkins y Kafka; configuración válida. | Arranque limpio, recursos reales y salud de todos los workers. El healthcheck de Dask worker comprueba conexión al scheduler, no que ese worker esté registrado. |
| E1.2 Despliegue | README identifica VM Azure, URL y `/health`; el endpoint incluye servicios y commit. | Obtener respuesta externa, comparar hash con main y evidenciar continuidad. `/health` puede decir Dask `ok` sin conocer número de workers. |
| E1.3 CI/CD | JCasC con pollSCM cada 2 minutos, credencial Kaggle, tests y despliegue; PR de fallo y reversión en Git. | Historial real de Jenkins; aislar candidato: actualmente `Start services`, datos y streaming modifican recursos del proyecto productivo antes de aprobar todas las pruebas. Comprobación final usa `/ping`, no salud integral. |
| E1.4 Ingesta | API Kaggle, lectura Dask particionada, limpieza cuantificada, unión y carga por lotes. | Revisar conteos propios y volumen procesado. `MAX_REVIEWS` no impone el mínimo de un millón y el muestreo fraccional registra conteos estimados. `_insert_partition` ignora cualquier `BulkWriteError`, no solo duplicados. |
| E1.5 Datos y vida | GeoJSON, índices, primera/última reseña, días, evento, censura y entrada tardía. | Inspeccionar datos reales. Para KM se necesita duración censurada hasta el corte del dataset, separada de `business_days`, que hoy mide primera–última reseña. |
| E1.6 Sentimiento | TF-IDF, NB y LR, partición por negocio, datos hasta T, métricas por clase, confusión y modelo versionado. | Recuperar métricas/modelos propios, distribución y ausencia de cruce. Se elige clasificador por F1 del conjunto denominado prueba: separar validación y prueba para una evaluación final independiente. Citar origen concreto de lo reutilizado de SentimentStream. |
| E1.7 Tasas | `reduceByKey`, validación contra `groupBy`, anualización, mínimo justificado e IC. | Ejecutar sobre datos propios y conservar tablas; comprobar índices y resultados públicos. |
| E1.8 Balanceo | 2/8/32/200 particiones, tres repeticiones, dispersión, conteos con/sin clave. | **Corregir operación medida:** hoy mide negativas por reseña; la guía pide medir la operación anterior de cierres/días-negocio. Mantener el benchmark de reseñas solo como complemento. |
| E1.9 API | Seis endpoints y operadores `$near`, `$geoWithin`, `$geoNear`; validaciones unitarias. | Pruebas reales geoespaciales y `/predict` en URL pública; datos variables y casos vacíos. |
| E1.10 Informe/demo | Plantilla de seis páginas y material de sustentación. | Incorporar resultados externos verificados, actualizar Azure en la plantilla, exportar y comprobar máximo de seis páginas; modificación en vivo por Jenkins y dominio de ambos integrantes. |

## Matriz de criterios: Entrega 2

| Criterio | Evidencia actual | Pendiente para aceptación |
| --- | --- | --- |
| E2.1 Integración | Kafka KRaft en mismo Compose, topic de tres particiones, API y health preparados. | Consumidor real y despliegue automático sin regresiones. `streaming` duerme indefinidamente si falta el archivo; no confundir contenedor activo con flujo operativo. |
| E2.2 Productor | Servicio y parámetros descritos; falta `streaming/producer.py`. | Cursor ordenado por fecha e ID, solo después de T, clave business_id, reloj acelerado configurable, confirmación de envío y manejo de errores. |
| E2.3 Inferencia | Registro de modelos y dependencias Spark/Kafka; falta `streaming/stream_job.py`. | Modelo E1 cargado una vez, schema explícito, clase/probabilidad/fecha de proceso, escritura idempotente, checkpoints y recuperación. |
| E2.4 Ventanas | Parámetros de 30 días, watermark siete días, geohash y regla documentada. | Agregación por tiempo del evento, geometría, comparación con ventana contigua, datos tardíos, deduplicación y pruebas de límites. |
| E2.5 Supervivencia | Campos base, API y dependencia lifelines; falta `jobs/survival.py`. | KM por categoría y zonas con `$geoWithin`, censura al corte, `entry`, n en riesgo, IC y mediana nullable; excluir conclusiones en grupos pequeños. |
| E2.6 Riesgo | Fórmula de etiqueta y funciones de variables; falta `jobs/risk_train.py`. | Variables históricas, competidores con `$geoNear`, etiqueta a doce meses, partición sin fuga, umbral en validación, AUC-ROC/AUC-PR/recall y modelo versionado. |
| E2.7 Alertas | Índices, rutas y actualización de estado como función auxiliar. | Integrar en `foreachBatch`, reconstrucción/reintento idempotente de variables, puntuar afectados, persistir riesgo y alertas con geometría, fecha y probabilidad. |
| E2.8 API/Power BI | Cuatro rutas nuevas y cuatro feeds tabulares; instrucciones de Power BI. | Generadores de las colecciones, pruebas con datos, archivo Power BI y cuatro visualizaciones conectadas; mostrar actualización durante demo. Un endpoint vacío no demuestra criterio completo. |
| E2.9 Jenkins | Etapa condicional `Streaming tests`; `tests/smoke` no existe. | Humo de cien mensajes y versión de modelo, recuperación/reintentos, prueba fallida que bloquee promoción sin afectar producción. |
| E2.10 Informe/demo | Plantilla final y preguntas de sustentación. | Resultados propios, reflexión con fuentes de reutilización, máximo diez páginas y demo completa pública. |

## Discrepancias y decisiones para revisión

| Referencia | Diferencia o ambigüedad | Decisión |
| --- | --- | --- |
| Guía vs diapositiva 11 | Guía: E1 6 nov, E2 24 nov. Diapositiva: 6–7 y 27–28 nov, con semana 23–24. | Rigen **6 y 24 de noviembre de 2026**. Confirmar cualquier cambio con docente. Para E2, reservar disponibilidad desde el 22 de noviembre, al menos 48 horas antes de la hora de sustentación. |
| Diapositiva 8 | Ejemplo `append` frente a instrucción de upsert. | ID estable por reseña y upsert real; checkpoint no reemplaza idempotencia. Verificar opciones contra conector 10.4.0 antes de implementar. |
| Diapositivas 9–10 y guía | Ejemplo KM no incluye `entry`, aunque censura y entrada tardía son obligatorias. | Implementar `entry` explícito y explicar que se corrige observación desde OBS_START, no apertura real desconocida. |
| Guía, vida vs censura | Vida aproximada primera–última reseña; abiertos sobreviven al corte del dataset. | Conservar `business_days` para la tasa exigida y campos separados de duración KM: cerrado hasta última reseña; abierto hasta corte. Documentar ambos ejes. |
| Guía E1.8 vs decisiones actuales | Se sustituyó benchmark de tasas por porcentaje negativo para tener más filas. | Restaurar benchmark exacto de cierres/días por negocio; documentar sobrecosto si el dataset es pequeño. |
| Guía E2.6 vs features estáticas Yelp | `stars` y `review_count` del negocio resumen todo el dataset. | Reconstruir medias y conteos solo con reseñas hasta T. `is_open` únicamente para etiqueta, nunca predictor. |
| Guía doce meses vs helper | `risk_label` usa 365 días por defecto. | Usar doce meses calendario; probar límites y año bisiesto. Para T actual ambos horizontes coinciden. |
| Requisito de ramas | La guía pide historial de aportes, ramas y PR; no prescribe literalmente dos nombres personales. | Dos ramas personales locales solicitadas por Andrés, conservando histórico existente y revisión cruzada documentada. |
| README vs árbol actual | README describe flujo E2 y pruebas de humo aún inexistentes. | Identificar explícitamente funciones disponibles y pendientes hasta verificar implementación. |

## Componentes, dependencias y contratos

| Componente | Depende de | Produce / consume |
| --- | --- | --- |
| Infraestructura y CI | VM, Docker, secretos externos, GitHub y Jenkins | Red Compose, almacenamiento persistente, URL, commit y salud verificable. |
| Ingesta Dask | Kaggle y alcance aprobado | `businesses`, `reviews`, parquet y conteos de calidad. |
| Vida y tasas | Ingesta | Vida, evento/censura, cierre por categoría/celda y benchmarks. |
| Sentimiento E1 | Reseñas hasta T y separación por negocio | Pipeline persistido y manifest de versión/corte/métricas. |
| Supervivencia | Vida, corte del dataset, OBS_START, polígonos | `survival` y `zones`, curvas, n en riesgo y medianas. |
| Riesgo | Datos históricos, sentimiento fijo, índice espacial | Estado hasta T, features, métricas, modelo y umbral de validación. |
| Productor | Reseñas después de T y Kafka reviews >=3 particiones | JSON estable, review_id, business_id como key, date, text, stars. |
| Consumidor | Kafka, ambos modelos, estado inicial y MongoDB | `reviews_stream`, `risk_scores`, `alerts`, progreso por partición. |
| Ventanas | Reseñas clasificadas y geohash de negocio | `windows` con geometría, inicio/fin, conteos, porcentaje y delta. |
| API y Power BI | Colecciones pobladas y health real | Consultas públicas y cuatro visualizaciones actualizables. |
| Aceptación | Todo lo anterior | Pruebas CI, evidencia real, informes y demo. |

Reglas del contrato a congelar antes de implementar:

1. T significa fin del día UTC `2020-06-30`: entrenamiento anterior a `2020-07-01T00:00:00Z`; productor desde ese instante. Evitar perder fracciones de segundo con límites `23:59:59`.
2. Kafka ordena por partición; el productor puede enviar en orden global, pero el consumidor no debe asumir orden global entre particiones. Ventanas usan `date`, nunca reloj de llegada.
3. Reseñas persistidas con ID estable `review_id`; riesgo y alerta con ID de negocio; ventana con geohash e inicio. Reprocesar un lote debe producir los mismos conteos y no sumar dos veces al estado.
4. Modelo de sentimiento fijo durante cada ejecución E2, con versión registrada. Registrar también versión de riesgo, corte, lista/orden de variables y umbral. No reentrenar dentro del flujo.
5. No inicializar estado de streaming con reseñas posteriores a T. Esas reseñas pueden servir para construir etiquetas, pero no variables iniciales.
6. Comparar hotspot con la ventana inmediatamente anterior de 30 días; una celda sin suficientes reseñas en cualquiera de las dos no activa hotspot. Umbral provisional heredado: diez puntos porcentuales y quince reseñas en cada ventana; revisar con datos.
7. Separar pruebas de humo de producción: cien mensajes identificables, topic/base/checkpoint de pruebas y misma imagen/modelos candidatos. No introducir reseñas ficticias en métricas públicas.
8. Contratos existentes de API se conservan; rutas E2 deben probar datos y filtros, no solo HTTP 200.

## Orden de implementación y reparto propuesto

Las ramas locales `andres/entrega2` y `julian/entrega2` parten de `ab0ffb2`. No se hizo push, merge ni cambio al servidor durante esta auditoría.

| Orden | Responsable propuesto | Trabajo y condición de salida |
| --- | --- | --- |
| 0 | Ambos | Recuperar resultados E1, acceso operativo VM/Jenkins y Power BI; verificar estado público y alcance. |
| 1 | Andrés, revisión Julián | Corregir benchmark de tasas y definir duración KM/censura; pruebas de fórmulas, límites temporales y entrada tardía. |
| 2 | Julián, revisión Andrés | Endurecer ingesta y aislamiento CI; imágenes/versiones identificables, candidato separado y salud final completa. |
| 3 | Andrés, revisión Julián | Validar modelo E1 y separación train/valid/test; conservar o generar versión reproducible sin usar después de T. |
| 4 | Andrés, revisión Julián | Implementar supervivencia por categoría y zona, con comparación sin entrada tardía como diagnóstico. |
| 5 | Julián, revisión Andrés | Implementar features históricas, competidores, entrenamiento/evaluación riesgo, umbral y estado inicial. |
| 6 | Julián, revisión Andrés | Implementar productor acelerado y tests de orden, corte, clave, errores y velocidad. Puede desarrollarse tras contratos sin esperar KM. |
| 7 | Andrés, revisión Julián | Implementar Structured Streaming, inferencia, ventanas e idempotencia de reseñas/estado/alertas; modelos cargados una vez. |
| 8 | Ambos | Conectar y probar diez endpoints, salud/lag y feeds; humo cien mensajes, versión de modelos, reintentos y fallo bloqueante en Jenkins. |
| 9 | Andrés, revisión Julián | Dashboard Power BI real: hotspots, alertas, KM y evolución negativa; verificar refresh sobre URL. |
| 10 | Ambos | Merge por PR, despliegue Jenkins, evidencias reales e informes E1 <=6 páginas / E2 <=10; ensayo integral. |

Los nombres de responsables indican reparto y revisión, no autoría ficticia. Andrés puede registrar su trabajo con la identidad confirmada. Los commits de Julián requieren que él revise y asuma efectivamente su contribución; no se necesitan sus contraseñas para preparar cambios revisables. Las aprobaciones cruzadas deben ser reales, como establece `docs/trabajo_en_equipo.md`.

## Pruebas y evidencia que cierran la entrega

- Unitarias: tasa anualizada, etiquetas/calendario, corte temporal, censura, entrada tardía, tendencia de seis meses, geometrías, hotspot, serialización y validaciones.
- Spark/Mongo: igualdad RDD/DataFrame; matrices y métricas reales; separación por negocio; parámetros y manifest del modelo; KM de casos conocidos con censurados y entradas tardías.
- Kafka/streaming: cien IDs clasificados con probabilidades/fecha; duplicados y reintento no inflan estado; ventanas por tiempo del evento y datos tardíos; geometría y alerta verificables; lag observado.
- API: seis rutas E1 y cuatro E2 contra candidato y URL pública; radio/categoría/polígono/ventana variables, errores 400 y ausencia de datos.
- CI/CD: merge detectado automáticamente, build verde, build rojo que no promueve, versión anterior operativa y hash correcto después de promoción.
- Entregables: tablas propias, capturas de Jenkins/health/dashboard, modelos y versiones, conteos, benchmarks, curvas, métricas riesgo y cuatro visualizaciones en archivo Power BI; informes paginados y fuentes de código reutilizado.

## Información pendiente

1. Ruta de resultados E1 y del `.pbix`, si existe; revisar antes de rehacer informes o entrenamiento.
2. Forma de acceso autorizada a VM/Jenkins (por ejemplo alias SSH ya configurado), sin copiar secretos al repositorio o chat.
3. Hora exacta de sustentación para cerrar ventana de 48 horas; fecha base 24 de noviembre.
4. URLs/commits de SentimentStream, KafkaMed y trabajo geoespacial reutilizados para citar y explicar qué cambió.

La auditoría no certifica que E1 esté completamente aprobada ni que E2 esté implementada. Define las brechas y las comprobaciones necesarias antes de declarar ambas listas.
