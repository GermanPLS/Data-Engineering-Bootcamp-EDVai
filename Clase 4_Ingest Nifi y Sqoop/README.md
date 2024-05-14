# INGEST con NIFI (niagara files) y SQOOP (sql+hadoop)


## sqoop

Sqoop es una aplicación con interfaz de línea de comando para transferir datos entre bases de datos relacionales y Hadoop.(Ingest)

![[imagen1](./Clase 4_Ingest Nifi y Sqoop/ingest sqoop.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/d3bef356fb838f7574e5aa6703bd39e724be83fa/Clase%204_Ingest%20Nifi%20y%20Sqoop/ingest%20sqoop.png)

Ejercicios Sqoop
================

Ejercicio 1 - Mostrar las tablas de la base de datos northwind.

```sh
sqoop list-databases \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres -P
```
![[imagen2](./Clase 4_Ingest Nifi y Sqoop/e1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/d3bef356fb838f7574e5aa6703bd39e724be83fa/Clase%204_Ingest%20Nifi%20y%20Sqoop/e1.png)



Ejercicio 2 - Mostrar los clientes de Argentina

```sh
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--query "select * from customers where country = 'Argentina' limit 10"
```
![[imagen3](./Clase 4_Ingest Nifi y Sqoop/e2.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/d3bef356fb838f7574e5aa6703bd39e724be83fa/Clase%204_Ingest%20Nifi%20y%20Sqoop/e2.png)


Ejercicio 3 - Importar un archivo .parquet que contenga toda la tabla orders. Luego ingestar el
archivo a HDFS (carpeta /sqoop/ingest)

```sh
sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--table orders \
--m 1 \
--target-dir /sqoop/ingest \
--as-parquetfile \
--delete-target-dir
```

Una vez que hayamos hecho el import nos quedará un archivo parquet en la
siguiente ruta: /sqoop/ingest

![[imagen4](./Clase 4_Ingest Nifi y Sqoop/e3.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/d3bef356fb838f7574e5aa6703bd39e724be83fa/Clase%204_Ingest%20Nifi%20y%20Sqoop/e3.png)

Por lo que luego ya podemos ingresar a spark para comenzar a crear un dataframe
en base a esa data.

```sh
pyspark

df = spark.read.parquet("/sqoop/ingest/*.parquet")

df.printSchema()

df.show(5)

```

![[imagen7](./Clase 4_Ingest Nifi y Sqoop/e3pyspark.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/f300160e993fb22d70d867b0bc45fd6636df27f4/Clase%204_Ingest%20Nifi%20y%20Sqoop/e3pyspark.png)

Ejercicio 4 -  Importar un archivo .parquet que contenga solo los productos con mas 20 unidades en
stock, de la tabla Products . Luego ingestar el archivo a HDFS (carpeta ingest)

```sh
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--query "select * from products where units_in_stock > 20 limit 10"

```
![[imagen5](./Clase 4_Ingest Nifi y Sqoop/e4.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/d3bef356fb838f7574e5aa6703bd39e724be83fa/Clase%204_Ingest%20Nifi%20y%20Sqoop/e4.png)

```sh
sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--table products \
--m 1 \
--target-dir /sqoop/ingest/southern \
--as-parquetfile \
--where "units_in_stock > 20" \
--delete-target-dir
```

![[imagen6](./Clase 4_Ingest Nifi y Sqoop/e41.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/e9c335e65b6e4f87aec927004a142543caf3be0e/Clase%204_Ingest%20Nifi%20y%20Sqoop/e41.png)



