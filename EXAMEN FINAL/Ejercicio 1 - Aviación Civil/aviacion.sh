rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://dataengineerpublic.blob.core.windows.net/data-engineer/2021-informe-ministerio.csv

wget -P /home/hadoop/landing https://dataengineerpublic.blob.core.windows.net/data-engineer/202206-informe-ministerio.csv

wget -P /home/hadoop/landing https://dataengineerpublic.blob.core.windows.net/data-engineer/aeropuertos_detalle.csv


/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest



