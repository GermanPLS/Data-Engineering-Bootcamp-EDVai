from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/drivers.csv")
df.createOrReplaceTempView("view_driver")
new_df = spark.sql("select cast(driverId as int), forename, surname, nationality  from view_driver ")



df1 = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")
df1.createOrReplaceTempView("view_results")
new_df1 = spark.sql("select cast(driverId as int), cast(points as float) from view_results ")
new_df1.createOrReplaceTempView("view_1")
new_df2 = spark.sql("select driverId, sum(points) as total_points from view_1 group by driverId order by total_points desc limit 10")



new_df.createOrReplaceTempView("view_driver_final")
new_df2.createOrReplaceTempView("view_result_final")

df_driver = spark.sql("select * from view_driver_final")
df_result = spark.sql("select * from view_result_final")


df_final = df_driver.join(df_result, on="driverId", how="inner")

df_final.createOrReplaceTempView("viaje_final")
df_f = spark.sql("select forename, surname, nationality, total_points from viaje_final order by total_points desc")

df_f .createOrReplaceTempView("viaje_final1")
spark.sql("insert into formula1.driver_results select* from viaje_final1")