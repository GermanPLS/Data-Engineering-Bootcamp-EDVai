# Google Cloud Platform Ingest - GCP



# Practica Ingest GCP


1. Crear un Bucket Regional standard en Finlandia llamado demo-bucket-edva.

![[imagen2](./Clase 5_Ingest_GCP/e1 gcp.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/4ef8fa6a873b9bb0fcc0a415f3aa694292b815d2/Clase%205_Ingest_GCP/e1%20gcp.png)

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

![[imagen3](./Clase 8_Ingest_GCP/e2 gcp.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/0f74b0ee4b0e2bf6bab08c12f19d3bd9bf8fe9a6/Clase%208_Ingest_GCP/e2%20gcp.png)





3. Utilizar el servicio de storage transfer para crear un job que copie los archivos
que se encuentran en data-bucket-demo-1 a demo-bucket-edvai.

- Vamos a Transferir los archivos del Bucket con el nombre data-bucket-demo-edvai-14 al Bucket llamado demo-bucket-edvai-00.
  
![[imagen4](./Clase 8_Ingest_GCP/e3 gcp.png)](https://github.com/GermanPLS/Bootcamp-Data-Engineering-----EDVai/blob/7f64b49d62c72ead1f3f84722c327a45cd90c4c8/Clase%208_Ingest_GCP/e3%20gcp.png)
