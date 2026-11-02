# Preparación de la sustentación

Cada integrante debe responder cualquiera de estas preguntas. Practíquenlas cruzadas: Julián responde las de las ramas de Andrés y viceversa.

| Pregunta probable | Respuesta corta | Dónde está |
|---|---|---|
| ¿Por qué la tasa no es "cerrados / total"? | Normaliza por exposición: un negocio que vivió 10 años aporta 10 veces más riesgo observado que uno de 1 año. Es la AFR de los discos. | `lifemap/metrics.py` |
| ¿Qué es un negocio censurado? | Abierto en la fecha de corte: solo sabemos que vivió *al menos* hasta ahí. Tratarlo como cierre subestima la vida; eliminarlo también. | `jobs/business_life.py` |
| ¿Qué es entrada tardía? | Negocios que ya existían al empezar la ventana (2010). Entran al conjunto en riesgo con su edad; sin esto, la curva queda sesgada hacia vidas largas. | `jobs/survival.py` |
| ¿Por qué partir por negocio? | Un negocio tiene vocabulario propio (nombre, platos); si cae en ambos lados el modelo "recuerda" el negocio y la métrica se infla. | `jobs/sentiment_train.py` |
| ¿Por qué F1 macro y no accuracy? | Las clases están desbalanceadas; accuracy premia predecir siempre "positivo". | idem |
| ¿Cómo validan el MapReduce? | Mismo resultado clave por clave que `groupBy().agg(sum)`; si difiere, el job falla. | `jobs/closure_rates.py` |
| ¿Por qué el sesgo con clave `city`? | Hash por ciudad: todas las filas de Philadelphia van a la misma partición; esa tarea domina el tiempo. Se ve en máx/media de `spark_partition_id()`. | `jobs/load_balancing.py` |
| ¿Qué pasa si falla una prueba en Jenkins? | El stage falla, el pipeline se detiene y la API pública no se reemplaza; `/health` sigue en el commit anterior. | `Jenkinsfile`, `docs/demo_build_fallido.md` |
| ¿Dónde vive el token de Kaggle? | Credencial `kaggle` de Jenkins (JCasC desde el `.env` del servidor); nunca en el repo. | `docker/jenkins/casc.yaml` |
| ¿Qué es el watermark? | Cuánto esperar datos tardíos por tiempo de evento (7 días) antes de cerrar una ventana y liberar su estado. | `streaming/stream_job.py` |
| ¿Por qué clave `business_id` en Kafka? | Todas las reseñas de un negocio van a la misma partición y conservan su orden. | `streaming/producer.py` |
| ¿Cómo evitan la fuga en el modelo de riesgo? | Variables solo con reseñas ≤ T; no se usan campos del archivo de negocios que son fotografía final. | `jobs/risk_train.py` |
| ¿Por qué AUC-PR? | Con pocos cierres, AUC-ROC puede ser alto aun con muchas falsas alarmas; AUC-PR se compara con la prevalencia. | idem |
| ¿Cómo se genera una alerta? | `foreachBatch` → actualiza acumulados del negocio → recalcula variables → puntúa → si supera el umbral, upsert en `alerts` con geometría, probabilidad y fecha. | `streaming/stream_job.py` (clase `Scorer`) |

## Modificación en vivo (E1.10)
El docente pedirá un cambio; el flujo es siempre: rama → cambio → prueba local → commit → PR → merge → Jenkins → verificar en la URL. Ensayen uno antes, por ejemplo: agregar el parámetro `min_stars` a `/businesses/near` (filtro `{"stars": {"$gte": min_stars}}` validado con `number(...)`), con su prueba en `tests/unit/test_api_validation.py`. Un ciclo completo tarda ~5 minutos (2 de pollSCM + build sin repetir el pipeline de datos).
