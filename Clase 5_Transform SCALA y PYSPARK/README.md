# TRANSFORM

Es el proceso de convertir, limpiar y estructurar datos en un formato utilizable que se pueda analizarpara respaldar los procesos de toma de decisiones.


![[imagen1](./Clase 5_Transform SCALA y PYSPARK/1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/5fcca545e40b33354e7296f0434d375944b4848f/Clase%205_Transform%20SCALA%20y%20PYSPARK/1.png)

## Transform Scala

```sh
spark-shell
val df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv")
df.show(5)
```
![[imagen2](./Clase 5_Transform SCALA y PYSPARK/2.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/5fcca545e40b33354e7296f0434d375944b4848f/Clase%205_Transform%20SCALA%20y%20PYSPARK/2.png)

Con este DataFrame podemos crear una vista temporal.

```sh
df.createOrReplaceTempView("tripdata_vista")
```
Le estoy diciendo a Spark que cree esta vista temporal llamada "tripdata_vista", que es como una tabla virtual, a la que se le puede hacer consulta SQL
como si fuera una tabla real.

Hacemos un nueva DataFrame

```sh
val new_df = spark.sql("select * from tripdata_vista where passenger_count = 1 and trip_distance > 5")
new_df.show(5)

# Para salir de la consola SCALA -->  :q 
```

![[imagen3](./Clase 5_Transform SCALA y PYSPARK/3.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/5fcca545e40b33354e7296f0434d375944b4848f/Clase%205_Transform%20SCALA%20y%20PYSPARK/3.png)

## Transform PySpark


```sh
pyspark
df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv"))
df.show(5)
```

```sh
df.createOrReplaceTempView("tripdata_vista")
new_df = spark.sql("select tpep_pickup_datetime, passenger_count, trip_distance from tripdata_vista where passenger_count > 1 and trip_distance between 1 and 10")
new_df.show(5)
```


 Pasamos/convertimos la distancia de milla a Kmts:

```sh
new_df.createOrReplaceTempView("vista_filtrada")
df_final = spark.sql("select tpep_pickup_datetime, passenger_count, trip_distance *1.6 as trip_distance_km from vista_filtrada")
df_final.show(5)
```



