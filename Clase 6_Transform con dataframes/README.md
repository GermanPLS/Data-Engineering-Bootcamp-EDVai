
# TRANSFORM  ( con DataFrames)

Hasta ahora Habiamos utilizado "vistas de SQL" para trabajar con SQL, ahora vamos a practicar gestionar DataFrames con Pyspark.



```sh

```

```sh
pyspark
```
```sh
df = spark.read.option("header", "true").parquet("/ingest/yellow_tripdata_2021-01.parquet")

df.show(5)

```
Generamos un nuevo dataFrame, donde estamos filtrando el dataframe anterior, df.

```sh
new_df = df.filter((df.passenger_count >1) & (df.trip_distance >=1) & (df.trip_distance <=10))
```

```sh
df_final = new_df.select(new_df.tep_pickup_datetime, new_df.passenger_count, (new_df.trip_distance*1.6).alias("trip_distance_km"))

df_final.show(5)
```