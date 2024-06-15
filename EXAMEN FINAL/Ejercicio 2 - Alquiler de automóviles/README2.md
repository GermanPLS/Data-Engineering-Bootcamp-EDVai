Alquiler de automóviles

Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y
reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se
encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de
automóvil, valoración de cada alquiler, etc.

Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos
listos para ser visualizados y responder las preguntas de negocio



#### Tarea 1

Crear en hive una database `car_rental_db` y dentro una tabla llamada `car_rental_analytics`, con estos campos:


| **campos**       | **tipo** |
|------------------|----------|
| fuelType         | string   |
| rating           | integer  |
| renterTripsTaken | integer  |
| reviewCount      | integer  |
| city             | string   |
| state_name       | string   |
| owner_id         | integer  |
| rate_daily       | integer  |
| make             | string   |
| model            | string   |
| year             | integer  |