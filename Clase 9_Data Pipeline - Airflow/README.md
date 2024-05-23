# Data pipelines

Una forma de tener mi flujo de informacion dentro de un proyecto de Big Data.

```js
•   ¿Cómo se representan?

        Mediante => Directed Acyclic Graphs (DAGS).
        Los DAGS son una representacion grafica de todo el flujo de informacion.


•   ¿Qué etapas existen?

        Ingesta, transformación, Almacenamiento y Análisis.


•   ¿Qué tipos de pipelines existen?

        DW pipeline, streaming pipeline, ML pipeline.
```

## DAGS

```js
● ¿Qué son?

    Son nodos(jobs)unidos por aristas, donde cada nodo va a ser un proceso de informacion, y las aristas ve van a decir como corre e flujo de informacion.


● ¿Para qué se utilizan?


    Se utilizan para modelar un pipeline de datos.


● ¿Cómo se utilizan? 

    Se modela el flujo de datos de mi proyecto de Big Data.
 ```   

 
 ### ETAPAS



### ORQUESTACION

La orquestación de datos automatiza los 
procesos relacionados con la gestión de datos, 
como ingerir datos de múltiples fuentes, 
combinarlos y prepararlos para el análisis de 
datos. Adicionalmente realiza supervisión de las 
tareas.

En la orquestación de datos se encuentra la creación de pipelines de datos y 
flujos de trabajo para mover datos de una ubicación a otra mientras se coordina 
la combinación, verificación y almacenamiento de esos datos para que sean 
útiles.

 La orquestación de datos moderna implica definir las tareas básicas dentro de 
un sistema de datos y ejecutar lo que se conoce como un grafo acíclico dirigido 
(DAG) que ilustra todas las tareas relevantes y su relación entre sí.


## Airflow

Apache Airflow es un software libre de orquestación de flujos de trabajo, que son creados a través de scripts de Python, y que pueden ser monitoreados haciendo uso de su interfaz de usuario.

Es importante notar que Airflow no es una herramienta ETL, es decir, el objetivo de Airflow no es manejar el dato , sino las tareas que se ocupen de este, siguiendo el orden y el flujo definido por el usuario.

#### Apache Airflow - Arquitectura

Airflow normalmente consiste en 5 componentes principales:
```js
        - 1) Web Server:

        se encarga de proveer la interfaz gráfica donde el usuario puede
        interactuar con los flujos de trabajo y revisar el estado de las tareas
        que los componen.


        - 2) Scheduler:

         es el componente encargado de planificar las ejecuciones de las tareas y 
         las pipelines definidas por el usuario. Cuando las tareas están listas 
         para ser ejecutadas, estas son enviadas al Executor.

        - 3)  Executor:
        
         define cómo las tareas van a ser ejecutadas y son enviadas a los Workers.

        Queue – define el orden en el cual las tareas serán ejecutadas.
        Es parte de el ejecutor en la arquitectura de un solo nodo


        - 4) Worker:
        
        es el proceso o subproceso que ejecuta las tareas. Dependiendo de la
        configuración del Executor, habrá uno o varios Workers que reciban 
        diferentes tareas. 


        - 5) Metastore:
        
        es una base de datos donde se guardan todos los metadatos de Airflow y de
        los flujos de trabajo definidos. Es utilizado por el Scheduler, el
        Executor y el Webserver para guardar sus estados(usualmente usa PostgreSQL) .
```
Apache Airflow - parámetros

Los principales parámetros de Airflow son:
```js

    ● Catchup (si está en TRUE) – Los DAG tienen una variable schedule_interval 
    que determina el intervalo en el que se ejecutan. Con el parámetro catchup, se
    programará una ejecución de DAG para cualquier intervalo que no se haya 
    ejecutado desde la última ejecución programada regularmente.

    ● Parallelism (default es 32) – determina el máximo número de tareas que
    pueden correrse en paralelo para toda la instancia de Airflow.

    ● DAG_concurrency (default es 16) – máximo número de tareas que pueden
    correrse concurrentemente por DAG.

    ● MAX_ACTIVE_RUNS_PER_DAG (default es 16) – máximo número de DAG corriendo por DAG
```


#### Apache Airflow - operadores


La unidad principal con la que Airflow define un flujo de trabajo es el Grafo Acíclico Dirigido (DAG). Los nodos del grafo son las diferentes tareas y las aristas dirigidas muestran las relaciones y dependencias entre ellas. La propiedad acíclica permite que el DAG sea ejecutado de principio a fin sin entrar en ningún bucle. 

Las tareas son las unidades básicas de ejecución. Están ordenadas según las dependencias anteriores y posteriores definidas en el DAG al que pertenecen. Una tarea se define a partir de la realización de un Operator, que sirven como plantillas para una funcionalidad específica.


Operadores:
-
```js
● BashOperator: ejecuta un comando bash.
● PythonOperator: invoca una función Python.
● EmailOperator: envía un email.
● SimpleHttpOperator: hace una petición HTTP.
● DatabaseOperator: MySqlOperator, SqliteOperator, PostgresOperator, 
  MsSqlOperator, OracleOperator, JdbcOperator, etc. (ejecuta una query SQL)
● Sensor: espera por un tiempo, fichero, fila de base de datos, objeto en S3…
● Dummy: no realizan ninguna acción, se utilizan para comenzar o finalizar un job

```


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

### Ejercicio 2

 En Hive, mostrar el esquema de airport_trips

```sql
describe airport_trips;

```


### Ejercicio 3

Crear un archivo .bash que permita descargar los archivos mencionados abajo e
ingestarlos en HDFS:

    - Yellow_tripdata_2021-01.parquet

    - Yellow_tripdata_2021-02.parquet


```sh
docker exec -it edvai_hadoop14 bash
su hadoop
cd /home/hadoop/scripts/
ls -1

# creo el scripts
cat > ingest_parquet.sh

chmod 777 ingest_parquet.sh

# ejecuto el scripts

./ingest_parquet.sh

```

ingest_parquet.sh

Realizo la Ingesta en HDFS /Ingest de los archivos de viajes de taxi NY en formato .parquet + archivo .cvs delos codigos Id de los viajes (para saber el ID de los aeropuertos y usar en ejercicio 4).


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

## Transformation.py

1) primero lo voy hacer en Jupyter notebook con pyspark, en el contenedor Hadoop

2) hago lo mismo pero genero un scripts .py en:

         /home/hadoop/scripts/transformation.py



```sh
# en cmd
# Ingreso al contenedor de Hadoop para usar Jupyter

docker start edvai_hadoop14
docker exec -it edvai_hadoop14 bash
su hadoop


# Cambiar los permisos del script:
chmod 777 /home/hadoop/scripts/pyspark_jupyter.sh

# ejecuto el script
./home/hadoop/scripts/pyspark_jupyter.sh

# Al ejecutar el script nos dara la ruta al cual debemos acceder:

    http://127.0.0.1:8889/tree?token=b55e24ad ...

Dentro de Jupyter:

ingresando a través de las carpetas a /home/hadoop/notebooks

Ahi adentro iniciomos un nuevo notebook -->  New y luego Python 3
```

### Jupyter Notebook

```sh

# creamos una SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder \
.master("spark://localhost:7077") \
.getOrCreate()


# Generamos un dataframe y leemos el primer archivo .parquet

df_1 = spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")

# Generamos otro dataframe y leemos el segundo archivo .parquet

df_2= spark.read.option("header", "true").parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")


# Realizo la union de 2 dataframe

df = df_1.union(df_2)
df.show(5)


# creo una vista y un dataFrame nuevo

df.createOrReplaceTempView("aeropuerto_vista")
new_df = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista ")
new_df.show(4)

+--------------------+-----------+------------+------------+
|tpep_pickup_datetime|airport_fee|payment_type|tolls_amount|
+--------------------+-----------+------------+------------+
| 2020-12-31 21:30:10|       null|           2|         0.0|
| 2020-12-31 21:51:20|       null|           2|         0.0|
| 2020-12-31 21:43:30|       null|           1|         0.0|
| 2020-12-31 21:15:48|       null|           1|         0.0|
+--------------------+-----------+------------+------------+
only showing top 4 rows

# hago un casteo

new_df1 = spark.sql("select tpep_pickup_datetime,  cast(airport_fee as float) , cast(payment_type as int), tolls_amount from aeropuerto_vista ")

# hago un filtrado  

new_df2 = spark.sql("select tpep_pickup_datetime, airport_fee, payment_type, tolls_amount  from aeropuerto_vista where payment_type = 2 and airport_fee > 0")

new_df2.show(4)

+--------------------+-----------+------------+------------+
|tpep_pickup_datetime|airport_fee|payment_type|tolls_amount|
+--------------------+-----------+------------+------------+
| 2021-02-21 02:36:21|       1.25|           2|         0.0|
+--------------------+-----------+------------+------------+

# creo una nueva vista y un nuevo dataframe para insertar los datos en la table airport_trips en Hive.


new_df2.createOrReplaceTempView("viaje_aeropuertofinal")
spark.sql("insert into tripdata.airport_trips select* from viaje_aeropuertofinal")


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