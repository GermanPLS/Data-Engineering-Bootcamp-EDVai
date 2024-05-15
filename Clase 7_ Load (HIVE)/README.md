# HIVE (Data Warehouse )

Hasta Ahora lo que hicimos fue la ingesta (EXTRACT) de un archivo .csv, que estaba en un repositorio ( por ejemplo: S3) en nuestro reositorio  HDFS( area landing / raw).

Luego hicimos la etapa de Transformaion con  Scala y Pyspark (TRANSFORM)

Ahora vamos a ver la etapa de LOAD, guardar la informacion que procesamos en nuestro DW.

Estamos haciendo ETL.


Continuando con el codigo de Transfrom con Pyspark:

```sh
pyspark
df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv"))
df.createOrReplaceTempView("tripdata_vista")
new_df = spark.sql("select tpep_pickup_datetime, passenger_count, trip_distance from tripdata_vista where passenger_count > 1 and trip_distance between 1 and 10")
new_df.show(5)
new_df.createOrReplaceTempView("vista_filtrada")
df_final = spark.sql("select tpep_pickup_datetime, passenger_count, trip_distance *1.6 as trip_distance_km from vista_filtrada")
df_final.show(5)
```
Vamos a generar una nueva vista

```sh
df_final.createOrReplaceTempView("tripdata_vista_km")
# Realizo un casteo ( covertimos un campo de un tipo a otro / aclaramos que tipo es un campo )
df_insert = spark.sql("select cast(tpep_pickup_datetime as timestamp), cast(passenger_count as integer), cast(trip_distance_km as float) from tripdata_vista_km")
```
Generamos una vista a partir del dataframe df_insert

```sh
df_insert.createOrReplaceTempView("tripdata_insert")
```
Estamos listos para insertar toda esta enformacion al DW.

en otra consola, ponemos HIVE ( entramos a hive para poder administrar el DW ):

```sh
show databases;
create database tripdata;
use tripdata;
```

```sh
show databases;
create database tripdata;
use tripdata;
```

Vamos a crear la tabla donde vamos a insertar nuestros dataframes:


```sh
# genero la tabla llamada tripdata_table_km en la base de datos tripdata

create external table if not exists tripdata.tripdata_table_km(tpep_pickup_datetime timestamp, passenger_count integer, trip_distance_km float)

    > comment 'tripdata  tabla en Kmts'
    > row format delimited
    > fields terminated by ','
    > stored as textfile
    > location '/table/tripdata';

```


vuelvo a pyspark para hacer el insert

```sh
spark.sql("insert into tripdata.tripdata_table_km select* from tripdata_insert")

```

vuelvo a Hive y veo los registros del DW.

```sh
select tpep_pickup_datetime, passenger_count, trip_distance_km from tripdata_table_km limit 10;

```


