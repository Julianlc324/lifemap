# Dashboard requerido por E2.8

Las consultas de `queries/` están listas para importarse en Power BI. **Aún no existe un dashboard validado ni un archivo `.pbix`.** No se presenta esta carpeta como evidencia de cumplimiento completo.

En Power BI Desktop, crear primero la consulta/parámetro `BaseUrl` usando `queries/BaseUrl.pq`. Crear cuatro consultas en blanco llamadas `Alertas`, `Hotspots`, `Supervivencia` y `TendenciaNegativa`; pegar el contenido del `.pq` correspondiente en el Editor avanzado. Los esquemas explícitos mantienen las columnas cuando la API devuelve una lista vacía.

| Visual | Fuente | Campos |
| --- | --- | --- |
| Mapa de hotspots | Hotspots | Latitud `lat`, longitud `lon`, tamaño `pct_negative`, filtro `hotspot=true` y última `window_start`. |
| Tabla de alertas activas | Alertas | `name`, `category`, `probability` como porcentaje, `event_date`. |
| Curvas de Kaplan-Meier | Supervivencia | X `t_years` sin resumir, Y `survival`, leyenda `group`, segmentador `group_type`, tooltip `at_risk`, `ci_low`, `ci_high`. |
| Evolución negativa | TendenciaNegativa | X `window_start` convertida a fecha, Y `pct_negative`, tooltip `reviews`. |

`probability` y `survival` son fracciones 0–1; `pct_negative` ya está en escala 0–100. No multiplicar por cien dos veces. Para curvas, filtrar grupos que se quieren comparar y usar eje continuo; no sumar supervivencias entre grupos.

Durante la demo, usar **Actualizar** con la API pública y productor activo. Las ventanas se publican al cerrar por watermark; al acelerar el histórico pueden observarse nuevas ventanas en segundos reales. El conector Web usa importación: esta solución no promete actualización automática de página por DirectQuery. Guardar el dashboard como `lifemap.pbix` y conservar dos capturas en momentos diferentes.

Esta tarea necesita un entorno Power BI disponible. El equipo confirmó que aún no tiene ninguno configurado.
