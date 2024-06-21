from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")

df2 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")

df_3 = df.union(df2)

df_3 = df_3.drop("Calidad dato")


df_3.createOrReplaceTempView("vista_aeropuerto")


new_df = spark.sql("""
    SELECT 
        to_date(`Fecha`, 'dd/MM/yyyy') AS fecha, 
        CAST(`Hora UTC` AS STRING) AS horaUTC,
        CAST(`Clase de Vuelo (todos los vuelos)` AS STRING) AS clase_vuelo,
        CAST(`Clasificación Vuelo` AS STRING) AS clasificacion_vuelo,
        CAST(`Tipo de Movimiento` AS STRING) AS tipo_movimiento,
        CAST(`Aeropuerto` AS STRING) AS aeropuerto,
        CAST(`Origen / Destino` AS STRING) AS origen_destino,
        CAST(`Aerolinea Nombre` AS STRING) AS aerolinea_nombre,
        CAST(`Aeronave` AS STRING) AS aeronave,
        CAST(`Pasajeros` AS INTEGER) AS pasajeros
    FROM vista_aeropuerto
""")


new_df = new_df.na.fill(value=0, subset=['pasajeros'])

new_df.write.mode("overwrite").saveAsTable("aviacion.aeropuerto_tabla")






 