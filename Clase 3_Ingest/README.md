HDFS - Haddop File System
-


PRACTICA INGEST
-

1. Ingresar a la consola Hadoop y luego cambiarse de usuario a Hadoop:

```cmd
C:\Users\Usuario>docker exec -it edvai_hadoop bash
root@a12c3f03e3c1:/# su hadoop
```

2. Ingresar al directorio /home/hadoops/scripts:

```cmd
hadoop@a12c3f03e3c1:/$ cd /home/hadoop/scripts/
hadoop@a12c3f03e3c1:~/scripts$
```

3.  Crear un script llamado landing.sh que baje el archivo
https://github.com/fpineyro/homework-0/blob/master/starwars.csv al
directorio temporal /home/hadoop/landing y luego lo envíe al
directorio de Hadoop file system (HDFS) /ingest. Antes de finalizar el
script que borre el archivo starwars.csv del directorio temporal
/home/hadoop/landing:

