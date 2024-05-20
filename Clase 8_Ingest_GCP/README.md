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




# Practica Ingest GCP


1. Crear un Bucket Regional standard en Finlandia llamado demo-bucket-edva.

![[imagen2](./Clase 8_Ingest_GCP/e1 gcp.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/8f89bb40f44451fc40268f2069bb4d0c1430aabe/Clase%208_Ingest_GCP/e1%20gcp.png)

2. Hacer ingest con la herramienta CLI Gsutil de 5 archivos csv en el bucket
data-bucket-demo-1 (mostrar mediante un print screen esta tarea).

creo un nuevo bucket en GCS con el nombre data-bucket-demo-edvai-14 y subo archivos .csv desde mi disco local por medio de Google cloud CLI o CLI/SDK.

creo un nuevo bucket en GCS y subo archivos .csv desde mi disco local por medio de Google cloud CLI o CLI/SDK:

Abro --> Google Cloud SDK Shell


```sh
gsutil help

gsutil cp D:/PYTHON/datasets/wine_reviews.csv gs://data-bucket-demo-edvai-14/

gsutil cp D:/PYTHON/datasets/train_essays_7_prompts_v2.csv gs://data-bucket-demo-edvai-14/

gsutil cp D:/PYTHON/datasets/resultado-de-encuestas-2016.csv gs://data-bucket-demo-edvai-14/

gsutil cp D:/PYTHON/datasets/weather gs://data-bucket-demo-edvai-14/

```






 gsutil cp D:/PYTHON/datasets/train_essays_7_prompts_v2.csv gs://data-bucket-demo-edvai-14/


gsutil cp D:/PYTHON/datasets/resultado-de-encuestas-2016.csv gs://data-bucket-demo-edvai-14/


gsutil cp D:/PYTHON/datasets/weather gs://data-bucket-demo-edvai-14/

3. Utilizar el servicio de storage transfer para crear un job que copie los archivos
que se encuentran en data-bucket-demo-1 a demo-bucket-edvai.
