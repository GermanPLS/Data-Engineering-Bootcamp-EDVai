# TRANSFORM

Es el proceso de convertir, limpiar y estructurar datos en un formato utilizable que se pueda analizarpara respaldar los procesos de toma de decisiones.

## Transform Scala

```sh
spark-shell
val df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.csv")
df.show(5)
```

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


## Transform PySpark
