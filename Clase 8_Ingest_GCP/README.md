# Google Cloud Platform Ingest - GCP




![[imagen1](./Clase 8_Ingest_GCP/GCP ingest.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/16a7a71bb361b75671bd1316b8c0246964e6752f/Clase%208_Ingest_GCP/GCP%20ingest.png)

En GCP hay varias maneras de hacer Ingest de datos, pero estas son las 4 mas importantes:

    • GCS Transfer tools

            - GS util
            - tambien se puede levantar archivos por interfaz grafica.

   
    • Transfer Service

            - me permite mover informacion de otras "nubes".
            - en GCP => HDFS se llama Google Cloud Storage (GCS)


    • Transfer Appliance            

            - Es cuando tenemos mucha cantidad de informacion ( > 10 tera)


    • Bigquery Data Transfer Service

            - me permite llevar informacion directamente hacia Big Query

            - BIGQUERY dentro de GCP es el DW ( como Hive).




Toda la informacion dentro GCP esta encriptada, ya sea informacion en transito o almanecada (es un ambiente seguro).



OLTP --> ES MAS INFORMACION TRANSACCIONAL, ES MAS PARA SISTEMAS, PARA OPERACIONES

        - NORMALIZADA

OLAP --> SON ANALITICAS, PARA INFORMACION
        - DESNORMALIZADAS


EVITAR ES TRATAR HACER JOIN DENTRO DEL DW, ES UNA OPERACION SUPER COSTOSA.


# Practica Ingest GCP


1. Crear un Bucket Regional standard en Finlandia llamado demo-bucket-edva.

        - Google Cloud Storage
            --> Buckets => Crear


BUCKET: es un contenedor de Objetos. Un Objeto es un archivo, entonces un Bucket(contenedor) es para guardar onjetos,como una carpeta raiz que almacena todos tus archivos.

2. Hacer ingest con la herramienta CLI Gsutil de 5 archivos csv en el bucket
data-bucket-demo-1 (mostrar mediante un print screen esta tarea).



3. Utilizar el servicio de storage transfer para crear un job que copie los archivos
que se encuentran en data-bucket-demo-1 a demo-bucket-edvai.
