## EJERCICIO 2

# Alquiler de automóviles

Una de las empresas líderes en alquileres de automóviles solicita una serie de dashboards y
reportes para poder basar sus decisiones en datos. Entre los indicadores mencionados se
encuentran total de alquileres, segmentación por tipo de combustible, lugar, marca y modelo de
automóvil, valoración de cada alquiler, etc.

Como Data Engineer debe crear y automatizar el pipeline para tener como resultado los datos
listos para ser visualizados y responder las preguntas de negocio



#### Tarea 

### 1. Crear en hive una database `car_rental_db` y dentro una tabla llamada `car_rental_analytics`, con estos campos:


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


### Resolucion:

```
show databases;
create database car_rental_db;
show databases;
use car_rental_db;
```

![alt text](imagenes/e1.png)
```
CREATE EXTERNAL TABLE car_rental_db.car_rental_analytics(fuelType STRING, rating INTEGER, renterTripsTaken INTEGER, reviewCount INTEGER, city STRING, state_name STRING, owner_id INTEGER, rate_daily INTEGER, make STRING, model STRING, year INTEGER)
COMMENT 'Car Rental_db DB - Car Rental Analytics'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/car_rental_analytics';
```
![alt text](imagenes/e111.png)

### 2. Crear script para el ingest de estos dos files :

   • https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D



   • https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B


Sugerencia: Descargar el segundo archivo con un comando similar al abajo mencionado, ya que al tener caracteres como & falla si no se le asignan comillas. Adicionalmente, el parámetro -O permite asignarle un nombre más legible al archivo descargado

    wget -P ruta_destino -O ruta_destino/nombre_archivo.csv ruta_al_archivo


### Resolucion:

Se crea archivo script  `alquiler.sh`, guardado en `/home/hadoops/scripts`, para realizar la ingesta de archivos en `hdfs dfs /ingest`.

```sh
rm -f /home/hadoop/landing/*

wget -O /home/hadoop/landing/car_rental_data.csv "https://edvaibucket.blob.core.windows.net/data-engineer-edvai/CarRentalData.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D"

 
wget -O /home/hadoop/landing/georef_usa_state.csv "https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"


/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest

```


![alt text](imagenes/e2.png)


### 3. Crear un script para tomar el archivo desde HDFS y hacer las siguientes transformaciones:

    ● En donde sea necesario, modificar los nombres de las columnas. Evitar espacios y puntos (reemplazar por _ ). Evitar nombres de columna largos

    ● Redondear los float de ‘rating’ y castear a int.

    ● Joinear ambos files

    ● Eliminar los registros con rating nulo

    ● Cambiar mayúsculas por minúsculas en ‘fuelType’

    ● Excluir el estado Texas

Finalmente insertar en Hive el resultado


### Resolucion:

 transformacion para la tabla `car_rental_analytics`, se genera el archivo `transform_renta.py`, guardado en /home/hadoops/scripts: 


```sh
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import round, lower
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/car_rental_data.csv")


df = df.toDF(*(c.replace('.', '_') for c in df.columns))
df = df.toDF(*(c.replace(' ', '_') for c in df.columns))

df = df.withColumn('rating', round('rating').cast('int'))

df.createOrReplaceTempView("vista_nueva")



df1 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/georef_usa_state.csv")

df1 = df1.toDF(*(c.replace('.', '_') for c in df1.columns))
df1 = df1.toDF(*(c.replace(' ', '_') for c in df1.columns))

df1 = df1.withColumnRenamed('United_States_Postal_Service_state_abbreviation', 'Postal_Service')


df3 = df.join(df1, df.location_state == df1.Postal_Service, 'left')

df3 = df3.na.drop(subset=["rating"])

df3 = df3.withColumn('fuelType', lower(df3.fuelType))

df3 = df3.filter(df3.Official_Name_State != 'Texas')


df3.createOrReplaceTempView("vista_nueva")


new_df = spark.sql("select cast(fuelType as string), cast(rating as integer), cast(renterTripsTaken as integer), cast(reviewCount as integer), cast(location_city as string) as city, cast(Official_Name_State as string) as state_name, cast(owner_id as integer), cast(rate_daily as integer), cast(vehicle_make as string) as make, cast(vehicle_model as string) as model, cast(vehicle_year as integer) as year from vista_nueva")

new_df.write.mode("overwrite").saveAsTable("car_rental_db.car_rental_analytics")

```


### 4.  Realizar un proceso automático en Airflow que orqueste los pipelines creados en los puntos anteriores. Crear dos tareas:

    a. Un DAG padre que ingente los archivos y luego llame al DAG hijo.

    b. Un DAG hijo que procese la información y la cargue en Hive.



### Resolucion:

Se crean los siguientes archivos :

 ## ---> [Dag Padre](dag1_renta.py)

 ## ---> [Dag Hijo](dag2_renta.py)  

![alt text](imagenes/e41.png)

![alt text](imagenes/e42.png) 

![alt text](imagenes/e43.png)


### 5. Por medio de consultas SQL al data-warehouse, mostrar:

    a. Cantidad de alquileres de autos, teniendo en cuenta sólo los vehículos
    ecológicos (fuelType hibrido o eléctrico) y con un rating de al menos 4.

    b. los 5 estados con menor cantidad de alquileres (crear visualización).

    c.  los 10 modelos (junto con su marca) de autos más rentados (crear visualización).

    d. Mostrar por año, cuántos alquileres se hicieron, teniendo en cuenta automóviles fabricados desde 2010 a 2015.

    e. as 5 ciudades con más alquileres de vehículos ecológicos (fuelType hibrido o electrico).

    f.  el promedio de reviews, segmentando por tipo de combustible.


### Resolucion:

a.
```sql
select count(*) as cantidad_alquileres
from car_rental_analytics
where
    (fueltype = 'electric' or fueltype = 'hybrid') and rating >= 4;
```
![alt text](imagenes/e5a.png)

b.
```sql
select
    state_name,
    count(*) as cantidad_alquileres
from
    car_rental_analytics
group by
    state_name
order by
    cantidad_alquileres asc
limit 5;
```
![alt text](imagenes/e5b.png)

c.
```sql
select
    make,
    model,
    count(*) as cantidad_alquileres
from
    car_rental_analytics
group by
    make,
    model
order by
    cantidad_alquileres desc
limit
    10;
```
![alt text](imagenes/e5c.png)

d.
```sql
select
    year,
    count(*) as cantidad_alquileres
from
    car_rental_analytics
where
    (year >= 2010) and (year <= 2015)
group by year
order by year asc;

```

![alt text](imagenes/e5d.png)


e.
```sql
select
    city,
    count(*) as cantidad_alquileres
from
    car_rental_analytics
where
    (fueltype == 'electric') or (fueltype == 'hybrid')
group by city
order by cantidad_alquileres desc
limit 5;

```
![alt text](imagenes/e5e.png)


f.
```sql
select
    fueltype,
    avg(reviewcount) as review_count_avg
from car_rental_analytics
group by fueltype
order by review_count_avg desc;
```

![alt text](imagenes/e5f.png)

### 6.  Elabore sus conclusiones y recomendaciones sobre este proyecto.

Estas conclusiones pueden ayudar a la empresa a tomar decisiones basadas en datos y mejorar su servicio y oferta de vehículos:


Si se observa una alta demanda y buenas valoraciones para vehículos híbridos y eléctricos, sería recomendable aumentar la disponibilidad de estos modelos.



Identificar factores que puedan estar influyendo en la baja cantidad de alquileres en ciertos estados y tomar medidas para mejorar la presencia y el servicio en esas áreas.



Mantener una flota adecuada de los modelos más rentados para satisfacer la demanda y evitar la falta de disponibilidad.


Continuar monitoreando las valoraciones de los clientes y realizar mejoras en el servicio y mantenimiento de los vehículos según el feedback recibido.

### 7. Proponer una arquitectura alternativa para este proceso ya sea con herramientas on premise o cloud (Si aplica).



![alt text](<../Ejercicio 1 - Aviación Civil/archivos/gcp.png>)
