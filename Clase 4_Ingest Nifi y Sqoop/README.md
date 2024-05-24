# INGEST con NIFI (niagara files) y SQOOP (sql+hadoop)



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


• En Pyspark
-

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

• En pyspark:
-

![[imagen8](./Clase 4_Ingest Nifi y Sqoop/e41pysprak.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/3110c867f60b61b3ff15b4384f43e458b5f58467/Clase%204_Ingest%20Nifi%20y%20Sqoop/e41pysprak.png)




## NIFI


Ejercicio 1 - En el shell de Nifi, crear un script .sh que descargue el archivo starwars.csv al directorio
/home/nifi/ingest (crearlo si es necesario). Ejecutarlo con ./home/nifi/ingest/ingest.sh

Shell NiFi
```sh
cat > ingest.sh
rm -f /home/nifi/ingest/starwars.csv
wget -P /home/nifi/ingest https://github.com/fpineyro/homework-0/blob/master/starwars.csv
# (Ctrl para salir)

# veo los permisos del archivo ingest.sh
ls -l ingest.sh

# resultado:
#  -rw-r--r-- 1 nifi nifi 127 May 15 12:44 ingest.sh

# necesito añadir permisos de ejecucion.Para hacer que el propietario (usuario nifi) pueda ejecutar el archivo:

chmod u+x ingest.sh

ls -l ingest.sh

# resultado:
# -rwxr--r-- 1 nifi nifi 127 May 15 12:44 ingest.sh

cd /
./home/nifi/ingest/ingest.sh

# resultado:

nifi@05c97bb3bfb9:/$ cd /home/nifi/ingest/
nifi@05c97bb3bfb9:~/ingest$ ls
ingest.sh  starwars.csv

```
```
Resumen de los permisos:

Propietario (nifi).
Grupo (nifi).
Otros usuarios.

Dar permisos de ejecución al propietario: chmod u+x ingest.sh
Dar permisos de ejecución al propietario, grupo y otros: ls -l ingest.sh

para habilitar todos los permisos:

nifi@05c97bb3bfb9:~/ingest$ chmod a+rwx ingest.sh
nifi@05c97bb3bfb9:~/ingest$ ls -l ingest.sh
-rwxrwxrwx 1 nifi nifi 127 May 15 12:44 ingest.sh

```

Ejercicio 2 - Usando procesos en Nifi:

a) tomar el archivo starwars.csv desde el directorio /home/nifi/ingest.
b) Mover el archivo starwars.csv desde el directorio anterior, a /home/nifi/bucket
(crear el directorio si es necesario)
c) Tomar nuevamente el archivo, ahora desde /home/nifi/bucket
d) Ingestarlo en HDFS/nifi (si es necesario, crear el directorio con hdfs dfs -mkdir/nifi )

Shell Hadoop
```sh
hdfs dfs -ls /

# veo que permisos tiene la carpeta nifi
# Resulto:
Found 8 items
drwxr-xr-x   - hadoop supergroup          0 2024-05-13 18:02 /ingest
drwxr-xr-x   - hadoop supergroup          0 2022-04-26 19:51 /inputs
drwxr-xr-x   - hadoop supergroup          0 2022-01-22 21:35 /logs
drwxrwxrwx   - hadoop supergroup          0 2024-05-15 09:17 /nifi
drwxr-xr-x   - hadoop supergroup          0 2024-05-14 09:14 /sqoop
drwxr-xr-x   - hadoop supergroup          0 2024-05-05 10:12 /table
drwxrwxr-x   - hadoop supergroup          0 2022-05-02 20:46 /tmp
drwxr-xr-x   - hadoop supergroup          0 2022-01-23 13:15 /user

# limpio o borro lo que hay en la carpeta NiFi

hdfs dfs -rm -f /nifi/*
```

```
# si los permisos no estan habilitados o falta crear la carpeta NiFi en HDFS :

hdfs dfs -mkdir /nifi
hdfs dfs -chmod 777 /nifi
```

Shell NiFi
```sh
# elimino el archivo de la carpeta bucket
/bucket$ rm starwars.csv
nifi@05c97bb3bfb9:~/bucket$ ls
nifi@05c97bb3bfb9:~/bucket$
```

Interfaz Grafica

![[imagen10](./Clase 4_Ingest Nifi y Sqoop/ingest Interfaz_nifi.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/3abceed99ee5d8789a2a34a47a4533597520cd11/Clase%204_Ingest%20Nifi%20y%20Sqoop/ingest%20Interfaz_nifi.png)

Shell NiFi

```sh
# Resultado: 

nifi@05c97bb3bfb9:~$ pwd
/home/nifi
nifi@05c97bb3bfb9:~$ ls
bucket  hadoop  ingest
nifi@05c97bb3bfb9:~$ cd bucket
nifi@05c97bb3bfb9:~/bucket$ ls
nifi@05c97bb3bfb9:~/bucket$ ls
ingest.sh  starwars.csv
nifi@05c97bb3bfb9:~/bucket$
```
![[imagen11](./Clase 4_Ingest Nifi y Sqoop/hdfs nifi.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/3abceed99ee5d8789a2a34a47a4533597520cd11/Clase%204_Ingest%20Nifi%20y%20Sqoop/hdfs%20nifi.png)

