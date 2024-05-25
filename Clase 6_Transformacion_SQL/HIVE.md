# Practica Hive

Consigna: Por cada ejercicio, escribir el código y agregar una captura de pantalla del resultado
obtenido.
Diccionario de datos:
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

## 1. En Hive, crear las siguientes tablas (internas) en la base de datos tripdata en hive:

    a. payments(VendorID, tpep_pickup_datetetime, payment_type, total_amount)
    b. passengers(tpep_pickup_datetetime, passenger_count, total_amount)
    c. tolls (tpep_pickup_datetetime, passenger_count, tolls_amount, total_amount)
    d. congestion (tpep_pickup_datetetime, passenger_count,congestion_surcharge,total_amount)
    e. distance (tpep_pickup_datetetime, passenger_count, trip_distance,total_amount)

```sql
CREATE TABLE tripdata.payments(VendorID INTEGER, tpep_pickup_datetime DATE, payment_type INTEGER, total_amount DOUBLE)
COMMENT 'Payments table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';
```
```sql
CREATE TABLE tripdata.passengers(tpep_pickup_datetime DATE, passenger_count INTEGER, total_amount DOUBLE)
COMMENT 'Passengers table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

```

```sql
CREATE TABLE tripdata.tolls(tpep_pickup_datetime DATE, passenger_count INTEGER, tolls_amount DOUBLE, total_amount DOUBLE)
COMMENT 'Tolls table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

```

```sql
CREATE TABLE tripdata.congestion(tpep_pickup_datetime DATE, passenger_count INTEGER, congestion_surcharge DOUBLE, total_amount DOUBLE)
COMMENT 'Congestion table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

```

```sql
CREATE TABLE tripdata.distance(tpep_pickup_datetime DATE, passenger_count INTEGER, trip_distance DOUBLE, total_amount DOUBLE)
COMMENT 'Distance table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

```
![[imagen1](./Clase 6_Transformacion_SQL/Imagenes/h1.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h1.png)
![[imagen2](./Clase 6_Transformacion_SQL/Imagenes/h11.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h11.png)

## Ejercicio 2. 

En Hive, hacer un ‘describe’ de las tablas passengers y distance.

![[imagen3](./Clase 6_Transformacion_SQL/Imagenes/h2.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h2.png)

## Ejercicio 3.

 Hacer ingest del file: Yellow_tripodata_2021-01.csv
https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv
Para los siguientes ejercicios, debes usar PySpark (obligatorio). Si deseas practicar más,
también puedes repetir los mismos en SQL (opcional).

```
wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest
```
![[imagen4](./Clase 6_Transformacion_SQL/Imagenes/h3.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h3.png)

## Ejercicio 4.

 (Opcional SQL) Generar una vista

```
 df.createOrReplaceTempView("tripdata_vista")
```
## Ejercicio 5.

 Insertar en la tabla payments (VendorID, tpep_pickup_datetetime, payment_type, total_amount) Solamente los pagos con tarjeta de crédito.

![[imagen5](./Clase 6_Transformacion_SQL/Imagenes/h5.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h5.png)

![[imagen6](./Clase 6_Transformacion_SQL/Imagenes/h51.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h51.png)

![[imagen7](./Clase 6_Transformacion_SQL/Imagenes/h52.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h52.png)

 ## Ejercicio 6.

 Insertar en la tabla passengers (tpep_pickup_datetetime, passenger_count,
total_amount) los registros cuya cantidad de pasajeros sea mayor a 2 y el total del viaje
cueste más de 8 dólares.

![[imagen8](./Clase 6_Transformacion_SQL/Imagenes/h6.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h6.png)

![[imagen9](./Clase 6_Transformacion_SQL/Imagenes/h61.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h61.png)

![[imagen10](./Clase 6_Transformacion_SQL/Imagenes/h62.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h62.png)


## Ejercicio 7.

Insertar en la tabla tolls (tpep_pickup_datetetime, passenger_count, tolls_amount,
total_amount) los registros que tengan pago de peajes mayores a 0.1 y cantidad de
pasajeros mayores a 1.

![[imagen11](./Clase 6_Transformacion_SQL/Imagenes/h7.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h7.png)

![[imagen12](./Clase 6_Transformacion_SQL/Imagenes/h71.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h71.png)

![[imagen13](./Clase 6_Transformacion_SQL/Imagenes/h72.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h72.png)

## Ejercicio 8.

Insertar en la tabla congestion (tpep_pickup_datetetime, passenger_count,
congestion_surcharge, total_amount) los registros que hayan tenido congestión en los
viajes en la fecha 2021-01-18.

```sh

new_df = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int), cast(congestion_surcharge as double), cast(total_amount as double) from tripdata_vista where CAST(tpep_pickup_datetime AS DATE) = '2021-01-18' and cast(congestion_surcharge as double) > 0 ")

```
![[imagen14](./Clase 6_Transformacion_SQL/Imagenes/h8.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h8.png)

## Ejercicio 9.

Insertar en la tabla distance (tpep_pickup_datetetime, passenger_count, trip_distance,
total_amount) los registros de la fecha 2020-12-31 que hayan tenido solamente un
pasajero (passenger_count = 1) y hayan recorrido más de 15 millas (trip_distance).

```SH
new_df = spark.sql("select cast(tpep_pickup_datetime as date), cast(passenger_count as int), cast(trip_distance as double), cast(total_amount as double) from tripdata_vista where CAST(tpep_pickup_datetime AS DATE) = '2020-12-31' and cast(trip_distance as double) > 15 and cast(passenger_count as int) = 1 ")
```

![[imagen15](./Clase 6_Transformacion_SQL/Imagenes/h9.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h9.png)

![[imagen16](./Clase 6_Transformacion_SQL/Imagenes/h91.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/h91.png)
