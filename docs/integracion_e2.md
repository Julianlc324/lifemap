# Integración de E2 y validación — 19 de noviembre de 2026

## Alcance y reparto

La integración incluye productor Kafka, riesgo de cierre, Structured Streaming y supervivencia. La rama `julian/entrega2` agrupa productor, riesgo e ingesta para revisión de Julián; `andres/entrega2` contiene la integración, streaming, supervivencia y pruebas para revisión cruzada. Los commits de esta integración están registrados por Andrés. La asignación de revisión no cambia la autoría registrada en Git.

## Correcciones implementadas

- Productor: corte exclusivo a medianoche, desempate por ID, parámetros finitos/positivos, mínimo tres particiones y comprobación de confirmaciones de Kafka. Conserva microsegundos de `date`.
- Inferencia: modelo E1 cargado una vez, compatibilidad de versiones sentimiento/riesgo/corte, validación de mensajes, fallo explícito si Kafka perdió offsets, checkpoint v2 separado del anterior.
- Idempotencia: `risk_features` conserva el estado inicial hasta T; `risk_state` se reconstruye con las reseñas únicas persistidas por ID. Un reintento después de un fallo parcial vuelve a escribir estado, puntajes y alertas sin incrementar acumulados dos veces.
- Ventanas: deduplicación por ID/tiempo del evento, watermark siete días, ventanas treinta días. Se publican ventanas cerradas (`append`), una vez avanzado el watermark; las últimas ventanas del dataset pueden quedar abiertas hasta recibir eventos posteriores. No se inventan eventos de producción para cerrarlas.
- Hotspots: primero persiste todos los conteos del lote y después compara cada ventana con su predecesora contigua; una actualización también repara la comparación de la sucesora.
- KM: duración de abiertos hasta corte del dataset; cerrados hasta última reseña, con entrada tardía desde OBS_START. Se exporta la curva en tiempos reales de eventos/entradas/censuras, con IC escalonados y número en riesgo; no se extrapolan probabilidades a años fuera del seguimiento.
- Riesgo: etiqueta de doce meses calendario, partición estratificada determinística por ID y umbral sin redondear antes de usarlo; conserva los modelos NB/LR de E1 y LR/GBT de E2 existentes.
- Balanceo: mide cierres/días-negocio, una fila por negocio, con las cuatro configuraciones, tres repeticiones y distribuciones por partición.
- Ingesta: no oculta errores Mongo diferentes de duplicado, valida mínimo de muestra y registra conteos efectivos.
- Jenkins candidato: imágenes por hash, base/modelos/datos por build, pruebas de flujo en topic/base temporales y promoción de API/streaming solo después de aprobarlas. Snapshot con parada breve del consumidor y reinicio mediante `trap`; la API sigue disponible. La migración desde checkpoints anteriores con historial requiere revisión explícita y no se ejecuta silenciosamente.

## Evidencia observada

1. La URL devolvió todos los servicios `ok`, commit `ab0ffb2`, 13.883 negocios y 1.166.192 reseñas. Véase `evidencias/health-2026-11-19.json`.
2. Se conservó el reporte real anterior a las correcciones en `evidencias/resultados-vm-2026-11-19.md`. Sus curvas y benchmark pertenecen a la implementación anterior; no presentarlos como resultados corregidos.
3. Pruebas en la VM, contenedor temporal `lifemap-candidate-smoke`, código montado de solo lectura, modelos reales también de solo lectura: **47 passed in 37.69s**. Incluye cien reseñas reales posteriores a T, repetición de las cien sin duplicación, modelos versionados y avance de watermark en topic de prueba para verificar ventanas de treinta días y geometrías.
4. Recalcular KM en base temporal produjo **316 curvas, 83 suficientes**. El benchmark corregido se ejecutó aparte sobre los 13.883 negocios; recuperar JSON al finalizar para documentar todos los tiempos. Modo de validación utilizado: Spark `local[2]` dentro de la VM, no confundirlo con benchmark del clúster final.
5. Compose valida su configuración. La validación remota del Jenkinsfile y el build completo están pendientes de recuperar conectividad con la VM.

## Pendientes para declarar ambas entregas completas

- Validar el build Jenkins completo sobre este candidato, luego PR con revisión cruzada real y promoción a main. Conservar build fallido y hash público posterior. No se ha desplegado este código.
- Ejecutar el benchmark en el clúster final y exportar sus mediciones. Recuperar resultados de las pruebas aisladas.
- Revisar y migrar checkpoints si ya hay datos en streaming antiguo; el pipeline detiene la promoción en ese caso. Tras una actualización de sentimiento, regenerar riesgo y sus estados de T.
- Completar el dashboard Power BI real: hay cinco consultas `.pq` listas, pero no un `.pbix` ni reporte publicado verificado. Requiere Power BI habilitado por el equipo.
- Preparar informes paginados E1 <=6 y E2 <=10 páginas con los resultados definitivos, curvas, citas de reutilización y capturas de la demo. Las plantillas no equivalen a informes terminados.
- Ensayar la demo y revisión de cualquier módulo por ambos integrantes; mantener la URL desde 48 horas antes de la sustentación.

## Referencias técnicas

- Spark Structured Streaming: https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html
- KaplanMeierFitter (`entry`, censura y mediana): https://lifelines.readthedocs.io/en/latest/fitters/univariate/KaplanMeierFitter.html
- Requisitos normativos: los dos Markdown de la carpeta superior, con prioridad para la guía.
