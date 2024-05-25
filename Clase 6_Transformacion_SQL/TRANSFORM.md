# Practica Transformacion_  sql

## Ejercicio 1.

 En el container de Nifi, crear un .sh que permita descargar el archivo yellow_tripdata_2021-01.parquet desde: 

wget -O /home/fpineyro/test/yellow_tripdata_2021-01.parquet
https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

y lo guarde en /home/nifi/ingest.

Ejecutarlo


![[imagen1](./Clase 6_Transformacion_SQL/Imagenes/e1 .png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e1%20.png)


## 2. Por medio de la interfaz gráfica de Nifi, crear un job que tenga dos procesos.

a) GetFile para obtener el archivo del punto 1 (/home/nifi/ingest)

b) putHDFS para ingestarlo a HDFS (directorio nifi)

![[imagen2](./Clase 6_Transformacion_SQL/Imagenes/e21.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e21.png)

![[imagen3](./Clase 6_Transformacion_SQL/Imagenes/e22.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e22.png)

## Ejercicio 3.

 Con el archivo ya ingestado en HDFS/nifi, escribir las consultas y agregar captura de pantalla del resultado. Para los ejercicios puedes usar SQL mediante la creación de una vista llamada yellow_tripdata.
 
También debes chequear el diccionario de datos por cualquier duda que tengas respecto a las columnas del archivo.




https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf


```sh

df = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/nifi/yellow_tripdata_2021-01.parquet")

df.createOrReplaceTempView("yellow_tripdata")

```


### 3.1) Mostrar los resultados siguientes

    a. VendorId Integer
    b. Tpep_pickup_datetime date
    c. Total_amount double
    d. Donde el total (total_amount sea menor a 10 dólares)


```sh
new_df = spark.sql("select  VendorId, tpep_pickup_datetime, total_amount from yellow_tripdata Where total_amount<10")

```

![[imagen4](./Clase 6_Transformacion_SQL/Imagenes/e31.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e31.png)

### 3.2) 
Mostrar los 10 dias que mas se recaudado dinero ( tpep_pickup_datetime, total_amount).


```sh
new_df = spark.sql("select cast(tpep_pickup_datetime as date) as tpep_pickup_datetime, sum(total_amount) as monto_total from yellow_tripdata group by date(tpep_pickup_datetime) order by monto_total desc limit 10")

```

![[imagen5](./Clase 6_Transformacion_SQL/Imagenes/e32.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e32.png)

### 3.3) 
Mostrar los 10 viajes que menos dinero recaudó en viajes mayores a 10 millas
(trip_distance, total_amount)
```sh
new_df = spark.sql("select  trip_distance, total_amount as monto_total from yellow_tripdata Where trip_distance > 10 order by monto_total asc limit 10")
```
![[imagen6](./Clase 6_Transformacion_SQL/Imagenes/e33.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e33.png)
### 3.4) 
Mostrar los viajes de más de dos pasajeros que hayan pagado con tarjeta de
crédito (mostrar solo las columnas trip_distance y tpep_pickup_datetime)

```sh
new_df = spark.sql("select  trip_distance, cast(tpep_pickup_datetime as date)  from yellow_tripdata Where payment_type = 1  and passenger_count > 2")
```
![[imagen7](./Clase 6_Transformacion_SQL/Imagenes/e34.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e34.png)

### 3.5) 
Mostrar los 7 viajes con mayor propina en distancias mayores a 10 millas (mostrar
campos tpep_pickup_datetime, trip_distance, passenger_count, tip_amount).

```sh
new_df = spark.sql("select  trip_distance, cast(tpep_pickup_datetime as date), passenger_count, tip_amount   from yellow_tripdata  Where trip_distance > 10 order by  tip_amount desc limit 7")
```
![[imagen8](./Clase 6_Transformacion_SQL/Imagenes/e35.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e35.png)

### 3.6) 
Mostrar para cada uno de los valores de RateCodeID, el monto total y el monto
promedio. Excluir los viajes en donde RateCodeID es ‘Group Ride’.
```sh
new_df = spark.sql("select RatecodeID, sum(total_amount), avg(total_amount) from yellow_tripdata where RatecodeID != 6 group by RatecodeID")
```
![[imagen9](./Clase 6_Transformacion_SQL/Imagenes/e36.png](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/04897d9270949473246727273667036a9dd22533/Clase%206_Transformacion_SQL/Imagenes/e36.png)

