HDFS - Haddop File System
-
Ingest con Scripts
-
![[imagen3](./Clase 3_Ingest/ingest csripts.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/7d45e221cb5ab5750f711526a182a5e8de1388fd/Clase%203_Ingest/ingest%20csripts.png)

> Podemos utilizar comandos de linux para hacer ingest de archivos.

```sh
docker exec -it edvai_hadoop bash

su hadoop
ls

cd /home/hadoop/
ls

echo --> Obtenemos los archivos con WGET

wget -P /home/hadoop/landing/ https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv
cd landing/
ls
echo --> Observo si se bajo el archivo en la area de landing.

echo --> Movemos los archivos a HDFS, con PUT
hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.csv /ingest
hdfs dfs -ls /ingest
```
![[imagen4](./Clase 3_Ingest/hadoop ingest.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/353014f8ef46b72815fcd103fef4267b3f126320/Clase%203_Ingest/hadoop%20ingest.png)

PRACTICA INGEST
-

1. Ingresar a la consola Hadoop y luego cambiarse de usuario a Hadoop:


![[imagen1](./Clase 3_Ingest/1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/5775e9b3fa32ba80675a5aeb87853f53ac3a1f98/Clase%203_Ingest/1.png)

2. Ingresar al directorio /home/hadoops/scripts:

![[imagen2](./Clase 3_Ingest/22.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/ada5ed35603b1481fb8c8b130b91852384f8948a/Clase%203_Ingest/22.png)

3.  Crear un script llamado landing.sh que baje el archivo https://github.com/fpineyro/homework-0/blob/master/starwars.csv al
directorio temporal /home/hadoop/landing y luego lo envíe al directorio de Hadoop file system (HDFS) /ingest. Antes de finalizar el
script que borre el archivo starwars.csv del directorio temporal /home/hadoop/landing:

```sh
docker exec -it edvai_hadoop bash

su hadoop
ls

cd /home/hadoop/
ls

cd scripts
ls -1

nano landing.sh

rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest

ls

```

4. Cambiar permisos para que el script pueda ser ejecutado
   
```sh
chmod 777 landing.sh
```

5. Ejecutar el script para que baje el archivo starwars.csv de Github y lo envíe al directorio /ingest de HDFS

```sh
./landing.sh
```

6. Verificar que el archivo se encuentre en el directorio /ingest de HDFS

```sh
hdfs dfs -ls /ingest
```
![[imagen5](./Clase 3_Ingest/hdfs starwars.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/f6be8ba8fafc77363f97845cb05ac29ecb7340cb/Clase%203_Ingest/hdfs%20starwars.png)

