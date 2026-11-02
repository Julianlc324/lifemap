# Dashboard en Power BI (Entrega 2)

El dashboard se conecta a la API desplegada (no a archivos), así que se actualiza mientras el productor publica.

## 1. Consultas (Inicio → Obtener datos → Consulta en blanco → Editor avanzado)

Crear un parámetro `BaseUrl` (Inicio → Administrar parámetros) con `https://<SU-DOMINIO>.ngrok-free.app`. El encabezado `ngrok-skip-browser-warning` evita la página de aviso de ngrok gratuito.

```powerquery
// Alertas
let
    Fuente = Json.Document(Web.Contents(BaseUrl, [RelativePath = "powerbi/alerts",
             Headers = [#"ngrok-skip-browser-warning" = "1"]])),
    Tabla = Table.FromRecords(Fuente),
    Tipos = Table.TransformColumnTypes(Tabla, {{"probability", type number}, {"lat", type number}, {"lon", type number}})
in
    Tipos
```

Repetir cambiando `RelativePath` (y los tipos):

| Consulta | `RelativePath` | Columnas clave |
|---|---|---|
| Hotspots | `powerbi/hotspots` | `geohash`, `window_start`, `pct_negative`, `delta_pp`, `hotspot`, `lat`, `lon` |
| Supervivencia | `powerbi/survival` | `group_type`, `group`, `t_years`, `survival`, `ci_low`, `ci_high`, `at_risk` |
| Tendencia negativa | `powerbi/negative_trend` | `window_start`, `pct_negative`, `reviews` |

Si alguna consulta llega vacía (por ejemplo, antes de iniciar el productor), Power BI no crea columnas: inicien el productor unos segundos antes de construir el dashboard.

## 2. Visualizaciones mínimas (las cuatro de la guía)

1. **Mapa de hotspots:** visual *Mapa* con `lat`/`lon` de Hotspots, tamaño = `pct_negative`, color = `hotspot`; filtro de página por la última `window_start`.
2. **Tabla de alertas activas:** `name`, `category`, `probability` (formato %), `event_date`; ordenada por probabilidad. Opcional: segundo mapa con `lat`/`lon` de Alertas.
3. **Curvas de supervivencia:** gráfico de líneas, eje X = `t_years`, eje Y = `survival`, leyenda = `group`; segmentador por `group_type`. Tooltip con `at_risk`.
4. **Evolución del % negativo:** gráfico de líneas, eje X = `window_start`, Y = `pct_negative`.

Tarjetas sugeridas: número de alertas activas, % negativo de la última ventana.

## 3. Actualización durante la demo

1. Iniciar el productor: `docker compose run --rm producer --days-per-tick 30 --tick-seconds 10`.
2. En Power BI Desktop pulsar **Inicio → Actualizar** cada 20–30 segundos: se ven nuevas ventanas, hotspots y alertas.
3. Guardar el archivo como `powerbi/lifemap.pbix` en el repositorio (rama `feature/dashboard`) y capturas en `docs/`.

Nota: la actualización automática de página de Power BI solo funciona con DirectQuery, que no está disponible para el conector Web; por eso se usa *Actualizar*.
