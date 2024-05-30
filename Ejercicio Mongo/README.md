
# Ejercicio Mongo DB



## Ejercicio 1. 

 Crear una base de datos llamada logs.

    use logs
   



## Ejercicio 2. 

Crear una colección llamada users

    logs> db.createCollection("users")
    { ok: 1 }



## Ejercicio 3

Insertar en la colección users los datos que se encuentran en:

 https://github.com/fpineyro/homework-0/blob/master/Logs_May24.json


    db.users.insertMany ( ...


    )


## Ejercicio 4 

Hacer ingest del siguiente csv en el directorio HDFS /ingest

https://raw.githubusercontent.com/fpineyro/homework-0/master/Logs_Abr24.csv


    wget -P /home/hadoop/landing/ https://raw.githubusercontent.com/fpineyro/homework-0/master/Logs_Abr24.csv

    hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest

    hdfs dfs -ls /ingest


resultado:

    Found 3 items
    -rw-r--r--   1 hadoop supergroup      62131 2024-05-29 19:38 /ingest/Logs_Abr24.csv
    -rw-r--r--   1 hadoop supergroup   21686067 2024-05-28 15:08 /ingest/yellow_tripdata_2021-01.parquet
    -rw-r--r--   1 hadoop supergroup   21777258 2024-05-28 15:08 /ingest/yellow_tripdata_2021-02.parquet


# Ejercicio 5

Realizar una comparación de los logs del mes de Abril de 2024 (CSV) y Mayo 2024
(Mongo) para ver si hay personas que se conectaron desde la misma dirección IP.

```sh
pyspark --conf "spark.mongodb.read.connection.uri=mongodb://mongoadmin:edvai@172.17.0.3:27017/?authSource=admin&authMechanism=SCRAM-SHA-1" \--packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1
```

```sh

df = spark.read.format("mongodb").option("database", "logs").option("collection", "users").load()
df.show(3)

+--------------------+--------------------+----------+------+---+--------------+---------+
|                 _id|               email|first_name|gender| id|    ip_address|last_name|
+--------------------+--------------------+----------+------+---+--------------+---------+
|6657ade737aec0968...|vhasley0@printfri...|      Vina|Female|  1| 67.119.206.64|   Hasley|
|6657ade737aec0968...|   bboord1@goo.ne.jp| Benedetto|  Male|  2|47.235.253.124|    Boord|
|6657ade737aec0968...|kocurran2@hubpage...|     Kitti|Female|  3| 138.50.139.15| O'Curran|
+--------------------+--------------------+----------+------+---+--------------+---------+
only showing top 3 rows


df1 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/Logs_Abr24.csv")

df1.show(3)
+---+----------+---------+--------------------+------+-------------+
| id|first_name|last_name|               email|gender|   ip_address|
+---+----------+---------+--------------------+------+-------------+
|  1|     Berna|   Rumgay| brumgay0@rediff.com|Female|123.35.136.95|
|  2|    Aharon|   Denver|adenver1@macromed...|  Male| 29.88.96.170|
|  3|   Pinchas| Bewshire|pbewshire2@cloudf...|  Male|   233.24.9.3|
+---+----------+---------+--------------------+------+-------------+
only showing top 3 rows




df.createOrReplaceTempView("vw_f")
df1.createOrReplaceTempView("vw_f1")

df2 = spark.sql("select b.id, b.last_name, b.email, b.ip_address from vw_f as a join vw_f1 as b on a.ip_address = b.ip_address")

df2.show(10)

+---+------------+--------------------+--------------+
| id|   last_name|               email|    ip_address|
+---+------------+--------------------+--------------+
|185|      Haggie|fhaggie54@weebly.com| 150.145.9.133|
|308|   Otterwell|potterwell8j@goog...|  38.30.69.179|
|358|       Cruse|icruse9x@clickban...|175.137.19.124|
|680|Bartholomieu|bbartholomieuiv@b...| 167.231.38.66|
|550|        Boor|   oboorf9@about.com| 13.220.160.93|
|615|     Maskill|amaskillh2@blogge...| 11.201.85.230|
|813|    Dryburgh|ldryburghmk@yale.edu| 100.98.143.29|
|961|    Masserel|emasserelqo@simpl...|193.35.255.175|
|994|      Derges|  bdergesrl@webs.com| 158.206.250.1|
+---+------------+--------------------+--------------+


df3 = spark.sql("select cast(id as int), last_name, email, ip_address from view_cast")

df3.createOrReplaceTempView("view_final")

spark.sql("insert into tripdata.addressip select * from view_final")

```

en Hive

```sh
 CREATE TABLE tripdata.addressip(id int, last_name  string, email string, ip_address string)
    > COMMENT 'addressip table'
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ',';
```

```sh
show tables;
OK
addressip
airport_trips
congestion
distance
passengers
payments
tolls
tripdata_distance_km
tripdata_table
tripdata_table_km
Time taken: 0.024 seconds, Fetched: 10 row(s)
```
```sh
hive> describe addressip;
OK
id                      int
last_name               string
email                   string
ip_address              string
Time taken: 0.03 seconds, Fetched: 4 row(s)

```
![alt text](hive.png)
