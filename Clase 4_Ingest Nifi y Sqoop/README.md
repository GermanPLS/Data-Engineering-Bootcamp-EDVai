# INGEST con NIFI (niagara files) y SQOOP (sql+hadoop)


## sqoop

Sqoop es una aplicación con interfaz de línea de comando para transferir datos entre bases de datos relacionales y Hadoop.(Ingest)

Ejercicios Sqoop
================

Ejercicio 1 - Mostrar las tablas de la base de datos northwind.

```sh
sqoop list-databases \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres -P
```

Ejercicio 2 - Mostrar los clientes de Argentina

```sh
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--query "select * from customers where country = 'Argentina' limit 10"
```

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


Ejercicio 4 -  Importar un archivo .parquet que contenga solo los productos con mas 20 unidades en
stock, de la tabla Products . Luego ingestar el archivo a HDFS (carpeta ingest)

```sh
sqoop eval \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--P \
--query "select * from products where units_in_stock > 20 limit 10"

```
