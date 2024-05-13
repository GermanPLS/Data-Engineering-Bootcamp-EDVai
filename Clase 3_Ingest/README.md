HDFS - Haddop File System
-
Ingest con Scripts
-


> Podemos utilizar comandos de linux para hacer ingest de archivos.
> 

PRACTICA INGEST
-

1. Ingresar a la consola Hadoop y luego cambiarse de usuario a Hadoop:


![[imagen1](./Clase 3_Ingest/1.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/5775e9b3fa32ba80675a5aeb87853f53ac3a1f98/Clase%203_Ingest/1.png)

2. Ingresar al directorio /home/hadoops/scripts:

![[imagen2](./Clase 3_Ingest/22.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/ada5ed35603b1481fb8c8b130b91852384f8948a/Clase%203_Ingest/22.png)

3.  Crear un script llamado landing.sh que baje el archivo
https://github.com/fpineyro/homework-0/blob/master/starwars.csv al
directorio temporal /home/hadoop/landing y luego lo envíe al
directorio de Hadoop file system (HDFS) /ingest. Antes de finalizar el
script que borre el archivo starwars.csv del directorio temporal
/home/hadoop/landing:

