# Áreas metropolitanas del Yelp Open Dataset

Si el docente asigna otra área, cambiar `METRO_NAME` y `METRO_BBOX` en `config/lifemap.env` y repetir el pipeline (`FORCE_STEPS=all`). Las cajas son aproximadas: después de la ingesta revisen el mapa de la página de inicio y el reporte de limpieza (regla `fuera_del_area`) y ajústenlas si dejan por fuera municipios del área.

| Área | `METRO_BBOX` (lat_min,lat_max,lon_min,lon_max) |
|---|---|
| Philadelphia (PA/NJ) | `39.70,40.30,-75.60,-74.90` |
| Tampa (FL) | `27.60,28.30,-82.90,-82.20` |
| Indianapolis (IN) | `39.60,40.10,-86.40,-85.90` |
| Nashville (TN) | `35.90,36.40,-87.10,-86.50` |
| Tucson (AZ) | `32.00,32.50,-111.20,-110.70` |
| New Orleans (LA) | `29.80,30.15,-90.30,-89.85` |
| Saint Louis (MO/IL) | `38.40,38.95,-90.60,-89.90` |
| Reno (NV) | `39.35,39.75,-120.00,-119.60` |
| Edmonton (AB) | `53.35,53.70,-113.75,-113.25` |
| Boise (ID) | `43.50,43.75,-116.45,-116.10` |
| Santa Barbara (CA) | `34.35,34.50,-119.95,-119.50` |

Familias con suficientes reseñas para el modelo de texto: `Restaurants`, `Food`, `Nightlife`, `Shopping`, `Beauty & Spas`, `Active Life`, `Hotels & Travel`, `Automotive`, `Home Services`, `Health & Medical`. Las familias pequeñas en áreas pequeñas pueden quedar con pocos cierres: revisen que las tasas superen el mínimo de días-negocio.
