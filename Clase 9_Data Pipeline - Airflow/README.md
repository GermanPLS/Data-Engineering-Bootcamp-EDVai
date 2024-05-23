

# Ejercitacion_Airflow


Consigna: Por cada ejercicio, escribir el código y agregar una captura de pantalla del resultado obtenido.

Diccionario de datos:

https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

### Ejercicio 1

 En Hive, crear la siguiente tabla (externa) en la base de datos tripdata:

a. airport_trips(tpep_pickup_datetetime, airport_fee, payment_type, tolls_amount,total_amount)

```sql
hive
show databases;
use tripdata;
show tables;

# genero la tabla llamada airports_trips en la base de datos tripdata

CREATE EXTERNAL TABLE tripdata.airport_trips (
    tpep_pickup_datetime DATE,
    airport_fee DOUBLE,
    payment_type INT,
    tolls_amount DOUBLE
)
COMMENT 'Tabla de viajes al aeropuerto'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/tripdata';


```
![[imagen6](./Clase 9_Data Pipeline - Airflow/imagenes/6  e1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/78e72279b8ce0afa7a67b091472da5bd4d56016d/Clase%209_Data%20Pipeline%20-%20Airflow/imagenes/6%20%20e1.png)

### Ejercicio 2

 En Hive, mostrar el esquema de airport_trips

```sql
describe airport_trips;

```
![[imagen7](./Clase 9_Data Pipeline - Airflow/imagenes/7  e2.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/78e72279b8ce0afa7a67b091472da5bd4d56016d/Clase%209_Data%20Pipeline%20-%20Airflow/imagenes/7%20%20e2.png)

### Ejercicio 3

Crear un archivo .bash que permita descargar los archivos mencionados abajo e
ingestarlos en HDFS:

    - Yellow_tripdata_2021-01.parquet

    - Yellow_tripdata_2021-02.parquet


```sh
docker exec -it edvai_hadoop bash
su hadoop
cd /home/hadoop/scripts/
ls -1

# creo el scripts
cat > ingest.sh

chmod 777 ingest.sh

# ejecuto el scripts

./ingest_parquet.sh

```

ingest_parquet.sh

Realizo la Ingesta en HDFS /Ingest de los archivos de viajes de taxi NY en formato .parquet 


```sh
rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet

/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest

```

```sh
hdfs dfs -ls /ingest
```

Resultado:
```sh
hadoop@d937765cbe7f:~/scripts$ hdfs dfs -ls /ingest
Found 2 items
-rw-r--r--   1 hadoop supergroup   21686067 2024-05-22 09:22 /ingest/yellow_tripdata_2021-01.parquet
-rw-r--r--   1 hadoop supergroup   21777258 2024-05-22 09:22 /ingest/yellow_tripdata_2021-02.parquet

```






### Ejercicio 4

Crear un archivo .py que permita, mediante Spark, crear un data frame uniendo los viajes del mes 01 y mes 02 del año 2021 y luego Insertar en la tabla airport_trips losviajes que tuvieron como inicio o destino aeropuertos, que hayan pagado con dinero.



# /home/hadoop/scripts/transformation.py

```sh
hadoop@d937765cbe7f:~/scripts$ cat transformation.py
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")

df_2= spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")

df = df_1.union(df_2)

df.createOrReplaceTempView("aeropuerto_vista")
new_df = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista ")

new_df1 = spark.sql("select tpep_pickup_datetime,  cast(airport_fee as float) , cast(payment_type as int), tolls_amount from aeropuerto_vista ")

new_df2 = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista where payment_type = 2 and airport_fee > 0")

new_df2.createOrReplaceTempView("viaje_aeropuertofinal")
spark.sql("insert into tripdata.airport_trips select* from viaje_aeropuertofinal")
hadoop@d937765cbe7f:~/scripts$


hadoop@d937765cbe7f:~/scripts$ ls -1
derby.log
ingest.sh
ingest_parquet.sh
pyspark_jupyter.sh
spark-warehouse
start-services.sh
transformation.py
hadoop@d937765cbe7f:~/scripts$

```

En hive ( en el entorno Hadoop con Jupyer con pyspark, tabla creada ejercicio 2)

```sql

hive> show tables;
OK
airport_trips
tripdata_table
Time taken: 0.046 seconds, Fetched: 2 row(s)
hive> describe airport_trips;
OK
tpep_pickup_datetime    date
airport_fee             double
payment_type            int
tolls_amount            double
Time taken: 0.044 seconds, Fetched: 4 row(s)
hive>

hive> select* from airport_trips;
OK
2021-02-21      1.25    2       0.0
Time taken: 0.076 seconds, Fetched: 1 row(s)
hive>
```


Ejercicio 5


Realizar un proceso automático en Airflow que orqueste los archivos creados en los
puntos 3 y 4. Correrlo y mostrar una captura de pantalla (del DAG y del resultado en labase de datos)


```sh
ingreso a:

/home/hadoop/airflow/dags/
ls

__pycache__  example-DAG.py  ingest-transform.py


hadoop@d937765cbe7f:~/airflow/dags$ cat ingest-transform.py
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='ingest-transform',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )


    ingest = BashOperator(
        task_id='ingest',
        bash_command='/usr/bin/sh /home/hadoop/scripts/ingest.sh ',
    )


    transform = BashOperator(
        task_id='transform',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transformation.py ',
    )


    ingest >> transform >>finaliza_proceso




if __name__ == "__main__":
    dag.cli()

hadoop@d937765cbe7f:~/airflow/dags$


```
```
hadoop@a12c3f03e3c1:~/scripts$ cat ingest.sh
rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet


/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*
```

```
hadoop@a12c3f03e3c1:~/scripts$ cat ingest.sh
rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.parquet

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-02.parquet


/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*
```

```
hadoop@a12c3f03e3c1:~/scripts$ cat transformation.py
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")

df_2= spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")

df = df_1.union(df_2)

df.createOrReplaceTempView("aeropuerto_vista")
new_df = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista ")

new_df1 = spark.sql("select tpep_pickup_datetime,  cast(airport_fee as float) , cast(payment_type as int), tolls_amount from aeropuerto_vista ")

new_df2 = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista where payment_type = 2 and airport_fee > 0")

new_df2.createOrReplaceTempView("viaje_aeropuertofinal")
spark.sql("insert into tripdata.airport_trips select* from viaje_aeropuertofinal")
```




```
hive> select* from airport_trips;
OK
2021-02-21      1.25    2       0.0
2021-02-21      1.25    2       0.0
Time taken: 0.071 seconds, Fetched: 2 row(s)
hive>
```
![[imagen9](./Clase 9_Data Pipeline - Airflow/imagenes/9 airflow1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/78e72279b8ce0afa7a67b091472da5bd4d56016d/Clase%209_Data%20Pipeline%20-%20Airflow/imagenes/9%20airflow1.png)


![[imagen10](./Clase 9_Data Pipeline - Airflow/imagenes/10 airflow2.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/78e72279b8ce0afa7a67b091472da5bd4d56016d/Clase%209_Data%20Pipeline%20-%20Airflow/imagenes/10%20airflow2.png)

