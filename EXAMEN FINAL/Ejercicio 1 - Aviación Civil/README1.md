Aviación Civil


La Administración Nacional de Aviación Civil necesita una serie de informes para elevar al
ministerio de transporte acerca de los aterrizajes y despegues en todo el territorio Argentino,
como puede ser: cuales aviones son los que más volaron, cuántos pasajeros volaron, ciudades
de partidas y aterrizajes entre fechas determinadas, etc.
Usted como data engineer deberá realizar un pipeline con esta información, automatizarlo y
realizar los análisis de datos solicitados que permita responder las preguntas de negocio, y
hacer sus recomendaciones con respecto al estado actual.

Listado de vuelos realizados:

https://datos.transporte.gob.ar/dataset/lista-aeropuertos


# TAREAS

### 1. Hacer ingest de los siguientes files relacionados con transporte aéreo de Argentina :


    2021:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D

    2022:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

    Aeropuertos_detalles:

    https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D

wget -P /home/hadoop/landing https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%
2FPLRe9Y%3D

### Resolucion:


Se crea archivo script `aviacion.sh`, guardado en /home/hadoops/scripts, para realizar la ingesta de archivos en hdfs dfs /ingest.

![alt text](archivos/e1.png)

----------------
### 2. Crear 2 tablas en el datawarehouse, una para los vuelos realizados en 2021 y 2022
#### (2021-informe-ministerio.csv y 202206-informe-ministerio) y otra tabla para el detalle de los aeropuertos (aeropuertos_detalle.csv)


### **Schema Tabla 1**

| **campos**             | **tipo** |
|------------------------|----------|
| fecha                  | date     |
| horaUTC                | string   |
| clase_de_vuelo         | string   |
| clasificacion_de_vuelo | string   |
| tipo_de_movimiento     | string   |
| aeropuerto             | string   |
| origen_destino         | string   |
| aerolinea_nombre       | string   |
| aeronave               | string   |
| pasajeros              | integer  |


---------
### **Schema Tabla 2**

| **campos**    | **tipo** |
|---------------|----------|
| aeropuerto    | string   |
| oac           | string   |
| iata          | string   |
| tipo          | string   |
| denominacion  | string   |
| coordenadas   | string   |
| latitud       | string   |
| longitud      | string   |
| elev          | string   |
| uom_elev      | string   |
| ref           | string   |
| distancia_ref | float    |
| direccion_ref | string   |
| condicion     | string   |
| control       | string   |
| region        | string   |
| uso           | string   |
| trafico       | string   |
| sna           | string   |
| concesionado  | string   |
| provincia     | string   |


### Resolucion:

En Hive creamos la dataBase Aviacion, para luego crear las tablas:

![alt text](archivos/e2.png)

#### Creamos Tabla 1:

```
CREATE EXTERNAL TABLE aviacion.aeropuerto_vuelos(fecha DATE, horaUTC STRING, clase_de_vuelo STRING, clasificacion_de_vuelo STRING, tipo_de_movimiento STRING, aeropuerto STRING, origen_destino STRING, aerolinea_nombre STRING, aeronave STRING, pasajeros INTEGER)
COMMENT ' Aeropuerto Vuelos'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_tabla';
```


#### Creamos Tabla 2:

```
CREATE EXTERNAL TABLE aviacion.aeropuerto_detalles(aeropuerto STRING, oac STRING, iata STRING, tipo STRING, denominacion STRING, coordenadas STRING, latitud STRING, longitud STRING, elev STRING, uom_elev STRING, ref STRING, distancia_ref DOUBLE, direccion_ref STRING, condicion STRING, control STRING, region STRING, uso STRING, trafico STRING, sna STRING, concesionado STRING, provincia STRING)
COMMENT ' Aeropuerto Detalles '
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/aviacion/aeropuerto_detalles';
```

![alt text](archivos/e21.png)