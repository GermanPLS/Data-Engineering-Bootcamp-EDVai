# INGEST con NIFI (niagara files) y SQOOP (sql+hadoop)


## SQOOP

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

En pyspark:

![[imagen8](./Clase 4_Ingest Nifi y Sqoop/e41pysprak.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/3110c867f60b61b3ff15b4384f43e458b5f58467/Clase%204_Ingest%20Nifi%20y%20Sqoop/e41pysprak.png)




## NIFI

shell NIFI

```bash
docker ps
docker start nifi
cd /home/nifi/

creamos 3 carpetas:

mkdir bucket
mkdir hadoop
mkdir ingest


# en bucket vamos a simular que es un repositorio o carpeta de informacion, vamos a bajar un archivo:

cd bucket

https://dataengineerpublic.blob.core.windows.net/data-engineer/starwars.csv

ls

cd hadoop

# Desde la consola de nifi, es necesario agregar dos archivos de configuración llamados core-site.xml y hdfs-site.xml al directorio /home/nifi/hadoop.

cat > core-site.xml

<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->


<configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>hdfs://172.17.0.3:9000</value>
        </property>
</configuration>



# (ctrl+d --> salir)





cat > hdfs-site.xml

<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->


<configuration>

        <property>
                <name>dfs.replication</name>
                <value>1</value>
        </property>

        <property>
                <name>dfs.name.dir</name>
                <value>file:///home/hadoop/hadoopdata/hdfs/namenode</value>
        </property>

        <property>
                <name>dfs.data.dir</name>
                <value>file:///home/hadoop/hadoopdata/hdfs/datanode</value>
        </property>
</configuration>

```


shell HADOOP


```sh
hdfs dfs -ls /
hdfs dfs -mkdir /nifi

# Para que Nifi pueda ingestar el archivo a HDFS, debe asignársele el permiso desde laconsola de Hadoop con el comando hdfs dfs -chmod 777 /nifi

hdfs dfs -chmod 777 /nifi

# vemos si se ingesto el archivo en HDFS

hdfs dfs -ls /nifi
```



Ejercicio 1 - En el shell de Nifi, crear un script .sh que descargue el archivo starwars.csv al directorio
/home/nifi/ingest (crearlo si es necesario). Ejecutarlo con ./home/nifi/ingest/ingest.sh



