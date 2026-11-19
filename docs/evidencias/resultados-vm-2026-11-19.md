# Resultados LifeMap - Philadelphia / Restaurants

## Limpieza con Dask (conteos antes y después de cada regla)

| Archivo | Regla | Antes | Eliminados | Después | Justificación |
|---|---|---|---|---|---|
| business | coordenadas_nulas | 150.346 | 0 | 150.346 | Sin latitud/longitud no hay GeoJSON ni consultas $near/$geoWithin. |
| business | coordenadas_fuera_de_rango | 150.346 | 0 | 150.346 | Latitud fuera de [-90,90] o longitud fuera de [-180,180] rompe el índice 2dsphere. |
| business | fuera_del_area | 150.346 | 113.134 | 37.212 | Alcance del equipo: solo el área metropolitana asignada (METRO_BBOX). |
| business | sin_categorias | 37.212 | 33 | 37.179 | Sin categorías no se puede asignar familia ni categoría principal. |
| business | fuera_de_la_familia | 37.179 | 23.296 | 13.883 | Alcance del equipo: solo la familia de categorías asignada (CATEGORY_FAMILY). |
| business | is_open_invalido | 13.883 | 0 | 13.883 | is_open define evento/censura; solo se aceptan 0 o 1. |
| business | business_id_duplicado | 13.883 | 0 | 13.883 | Un negocio duplicado contaría dos veces sus días-negocio y su cierre. |
| review | negocio_fuera_de_alcance | 6.990.280 | 5.824.088 | 1.166.192 | La reseña debe unirse a un negocio limpio del área y la familia (unión reseña–negocio). |
| review | sin_texto | 1.166.192 | 0 | 1.166.192 | Sin texto no hay entrada para TF-IDF ni sentimiento. |
| review | estrellas_invalidas | 1.166.192 | 0 | 1.166.192 | La etiqueta de sentimiento se deriva de 1 a 5 estrellas. |
| review | fecha_invalida | 1.166.192 | 0 | 1.166.192 | La fecha define la vida del negocio, las ventanas y la partición en T. |
| review | review_id_duplicado | 1.166.192 | 0 | 1.166.192 | Una reseña duplicada sesga sentimiento y conteos por ventana. |

Distribución de la etiqueta de sentimiento (todas las reseñas): negativo: 244.537, neutral: 149.159, positivo: 772.496

## Vida de los negocios

| Negocios | Cierres (evento) | Censurados | Entrada tardía | Días-negocio | Mediana días | Corte del dataset |
|---|---|---|---|---|---|---|
| 13.883 | 4.956 | 8.927 | 4.105 | 35.359.777 | 2.416 | 2022-01-19 |

## Modelo de sentimiento (prueba, partición por negocio)

| Clasificador | Clase | Precision | Recall | F1 | Soporte |
|---|---|---|---|---|---|
| naive_bayes | negativo | 0.7169 | 0.7383 | 0.7274 | 43.656 |
| naive_bayes | neutral | 0.3601 | 0.5342 | 0.4302 | 27.977 |
| naive_bayes | positivo | 0.9306 | 0.8348 | 0.8801 | 144.037 |
| logistic_regression (elegido) | negativo | 0.8036 | 0.8097 | 0.8066 | 43.656 |
| logistic_regression (elegido) | neutral | 0.5007 | 0.3207 | 0.391 | 27.977 |
| logistic_regression (elegido) | positivo | 0.8902 | 0.9503 | 0.9193 | 144.037 |

**naive_bayes**: accuracy 0.7762, F1 macro 0.6792. Matriz de confusión (filas = real ['negativo', 'neutral', 'positivo'], columnas = predicho):

| real \ pred | negativo | neutral | positivo |
|---|---|---|---|
| negativo | 32230 | 9629 | 1797 |
| neutral | 5857 | 14946 | 7174 |
| positivo | 6870 | 16932 | 120235 |

**logistic_regression**: accuracy 0.8402, F1 macro 0.7056. Matriz de confusión (filas = real ['negativo', 'neutral', 'positivo'], columnas = predicho):

| real \ pred | negativo | neutral | positivo |
|---|---|---|---|
| negativo | 35347 | 4099 | 4210 |
| neutral | 6330 | 8972 | 12675 |
| positivo | 2310 | 4848 | 136879 |

## Tasa de cierre anualizada por categoría principal (top 10, >= 18.250 días-negocio)

| Clave | Negocios | Cierres | Años-negocio | Tasa % | IC 95 % |
|---|---|---|---|---|---|
| Falafel | 14 | 9 | 54.11 | 16.632 | 7.605 – 31.573 |
| Buffets | 52 | 40 | 298.75 | 13.389 | 9.565 – 18.232 |
| Cajun/Creole | 40 | 20 | 153.53 | 13.027 | 7.957 – 20.119 |
| Creperies | 11 | 7 | 55.88 | 12.526 | 5.036 – 25.809 |
| Moroccan | 11 | 7 | 59.71 | 11.723 | 4.713 – 24.155 |
| Indian | 156 | 78 | 707.0 | 11.033 | 8.721 – 13.769 |
| Szechuan | 13 | 6 | 55.67 | 10.778 | 3.955 – 23.459 |
| French | 72 | 50 | 471.85 | 10.597 | 7.865 – 13.97 |
| Middle Eastern | 57 | 31 | 298.31 | 10.392 | 7.061 – 14.75 |
| Restaurants | 41 | 23 | 229.59 | 10.018 | 6.35 – 15.032 |

## Tasa de cierre anualizada por celda geohash (top 10, >= 18.250 días-negocio)

| Clave | Negocios | Cierres | Años-negocio | Tasa % | IC 95 % |
|---|---|---|---|---|---|
| dr4sw | 15 | 8 | 86.18 | 9.283 | 4.008 – 18.291 |
| dr49v | 13 | 5 | 56.09 | 8.914 | 2.894 – 20.803 |
| dr4kf | 10 | 5 | 57.15 | 8.749 | 2.841 – 20.417 |
| dr49y | 10 | 5 | 57.53 | 8.692 | 2.822 – 20.284 |
| dr46b | 60 | 29 | 336.42 | 8.62 | 5.773 – 12.38 |
| dr4kr | 22 | 12 | 140.01 | 8.571 | 4.429 – 14.971 |
| dr4s7 | 15 | 8 | 93.99 | 8.512 | 3.675 – 16.771 |
| dr47b | 48 | 25 | 301.96 | 8.279 | 5.358 – 12.222 |
| dr4k5 | 19 | 9 | 119.46 | 7.534 | 3.445 – 14.301 |
| dr4s4 | 100 | 46 | 622.56 | 7.389 | 5.41 – 9.856 |

## Balanceo de carga: tiempos (3 repeticiones)

| Estrategia | Particiones | Tiempos (s) | Media (s) | Desv. est. (s) |
|---|---|---|---|---|
| con clave: main_category | 2 | 6.213, 6.426, 7.057 | 6.565 | 0.439 |
| con clave: main_category | 8 | 7.157, 7.575, 6.786 | 7.173 | 0.395 |
| con clave: main_category | 32 | 8.531, 9.942, 8.881 | 9.118 | 0.735 |
| con clave: main_category | 200 | 24.064, 24.158, 24.547 | 24.256 | 0.256 |
| sin clave | 2 | 8.232, 6.997, 6.987 | 7.405 | 0.716 |
| sin clave | 8 | 8.162, 8.223, 7.513 | 7.966 | 0.394 |
| sin clave | 32 | 9.635, 10.491, 9.604 | 9.91 | 0.504 |
| sin clave | 200 | 34.279, 34.257, 34.514 | 34.35 | 0.142 |

## Balanceo de carga: filas por partición (spark_partition_id)

| Particiones | Clave | No vacías | Mín | Máx | Media | Desv. | Máx/Media |
|---|---|---|---|---|---|---|---|
| 2 | city | 2 | 168.714 | 997.478 | 583.096 | 414.382 | 1.71 |
| 2 | main_category | 2 | 538.994 | 627.198 | 583.096 | 44.102 | 1.08 |
| 2 | sin clave | 2 | 583.087 | 583.105 | 583.096 | 9 | 1.0 |
| 8 | city | 8 | 29.524 | 774.209 | 145.774 | 238.778 | 5.31 |
| 8 | main_category | 8 | 40.059 | 244.435 | 145.774 | 69.096 | 1.68 |
| 8 | sin clave | 8 | 145.768 | 145.781 | 145.774 | 4 | 1.0 |
| 32 | city | 32 | 2.308 | 720.047 | 36.444 | 123.066 | 19.76 |
| 32 | main_category | 32 | 1.582 | 125.530 | 36.444 | 35.187 | 3.44 |
| 32 | sin clave | 32 | 36.438 | 36.449 | 36.444 | 3 | 1.0 |
| 200 | city | 174 | 0 | 687.382 | 5.831 | 48.488 | 117.88 |
| 200 | main_category | 165 | 0 | 95.139 | 5.831 | 13.596 | 16.32 |
| 200 | sin clave | 200 | 5.818 | 5.846 | 5.831 | 8 | 1.0 |

## Supervivencia (Kaplan-Meier con censura y entrada tardía)

| Tipo | Grupo | n | Eventos | Mediana (años) | S(1 año) | S(5 años) | Suficiente |
|---|---|---|---|---|---|---|---|
| category | Pizza | 1297 | 385 | 14.62 | 0.9676 | 0.8138 | sí |
| category | Sandwiches | 760 | 261 | 14.81 | 0.9711 | 0.7837 | sí |
| category | Italian | 699 | 303 | 10.92 | 0.9595 | 0.7266 | sí |
| category | Chinese | 671 | 172 | no se alcanza | 0.9693 | 0.867 | sí |
| category | American (Traditional) | 653 | 241 | 12.83 | 0.9442 | 0.749 | sí |
| category | American (New) | 588 | 297 | 8.73 | 0.9297 | 0.6357 | sí |
| category | Mexican | 455 | 153 | 12.94 | 0.9431 | 0.7142 | sí |
| category | Breakfast & Brunch | 451 | 161 | 13.04 | 0.9295 | 0.729 | sí |
| category | Coffee & Tea | 387 | 123 | 14.81 | 0.9684 | 0.796 | sí |
| category | Fast Food | 378 | 85 | no se alcanza | 0.973 | 0.8869 | sí |
| category | Burgers | 350 | 122 | 12.65 | 0.9527 | 0.7647 | sí |
| category | Delis | 299 | 92 | no se alcanza | 0.9698 | 0.815 | sí |
| category | Seafood | 299 | 98 | 14.57 | 0.9191 | 0.7121 | sí |
| category | Bakeries | 217 | 66 | no se alcanza | 0.9658 | 0.8013 | sí |
| category | Japanese | 217 | 82 | 11.64 | 0.9359 | 0.7293 | sí |
| category | Sushi Bars | 188 | 67 | 12.2 | 0.9511 | 0.7668 | sí |
| category | Cafes | 184 | 75 | 7.32 | 0.9615 | 0.6238 | sí |
| category | Diners | 180 | 91 | 9.19 | 0.8988 | 0.6616 | sí |
| category | Chicken Wings | 177 | 39 | no se alcanza | 0.9746 | 0.8702 | sí |
| category | Salad | 161 | 50 | 13.88 | 0.9213 | 0.7223 | sí |
| category | Indian | 155 | 77 | 4.08 | 0.8854 | 0.4759 | sí |
| category | Caterers | 154 | 51 | 14.31 | 0.9538 | 0.7984 | sí |
| category | Asian Fusion | 147 | 66 | 7.4 | 0.8627 | 0.6197 | sí |
| category | Barbeque | 140 | 64 | 7.99 | 0.8485 | 0.6084 | sí |
| category | Thai | 140 | 52 | 11.97 | 0.9037 | 0.717 | sí |
| category | Mediterranean | 136 | 62 | 8.37 | 0.8788 | 0.6392 | sí |
| category | Specialty Food | 133 | 48 | no se alcanza | 0.9364 | 0.712 | sí |
| category | Vietnamese | 131 | 63 | 6.65 | 0.9306 | 0.6475 | sí |
| category | Steakhouses | 128 | 39 | 14.58 | 0.9411 | 0.814 | sí |
| category | Pubs | 122 | 39 | 15.22 | 0.9697 | 0.8675 | sí |
| category | Cheesesteaks | 114 | 38 | no se alcanza | 0.9531 | 0.772 | sí |
| category | Sports Bars | 102 | 20 | no se alcanza | 0.9722 | 0.8974 | sí |
| category | Vegetarian | 102 | 43 | 11.2 | 0.9495 | 0.6578 | sí |
| category | Desserts | 94 | 24 | no se alcanza | 0.9263 | 0.7867 | sí |
| category | Bagels | 90 | 31 | 14.04 | 0.971 | 0.7991 | sí |
| category | Grocery | 90 | 17 | no se alcanza | 0.9863 | 0.899 | sí |
| category | Caribbean | 89 | 23 | no se alcanza | 0.9628 | 0.7213 | sí |
| category | Ice Cream & Frozen Yogurt | 86 | 36 | 9.94 | 0.9445 | 0.698 | sí |
| category | Korean | 84 | 44 | 6.74 | 0.8714 | 0.5747 | sí |
| category | Soup | 81 | 25 | 12.19 | 0.9859 | 0.8515 | sí |

## Modelo de riesgo de cierre (T = 2020-06-30, conjunto de prueba)

| Clasificador | AUC-ROC | AUC-PR | Prevalencia (AUC-PR azar) | Umbral | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|
| logistic_regression (elegido) | 0.6862 | 0.1295 | 0.0462 | 0.5266 | 0.0805 | 0.4865 | 0.1382 |
| gbt | 0.6163 | 0.0927 | 0.0462 | 0.1221 | 0.0537 | 0.8108 | 0.1008 |
