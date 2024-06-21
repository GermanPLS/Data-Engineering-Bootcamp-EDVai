
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import round, lower
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/car_rental_data.csv")


df = df.toDF(*(c.replace('.', '_') for c in df.columns))
df = df.toDF(*(c.replace(' ', '_') for c in df.columns))

df = df.withColumn('rating', round('rating').cast('int'))

df.createOrReplaceTempView("vista_nueva")



df1 = spark.read.option("header", "true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/georef_usa_state.csv")

df1 = df1.toDF(*(c.replace('.', '_') for c in df1.columns))
df1 = df1.toDF(*(c.replace(' ', '_') for c in df1.columns))

df1 = df1.withColumnRenamed('United_States_Postal_Service_state_abbreviation', 'Postal_Service')


df3 = df.join(df1, df.location_state == df1.Postal_Service, 'left')

df3 = df3.na.drop(subset=["rating"])

df3 = df3.withColumn('fuelType', lower(df3.fuelType))

df3 = df3.filter(df3.Official_Name_State != 'Texas')


df3.createOrReplaceTempView("vista_nueva")


new_df = spark.sql("select cast(fuelType as string), cast(rating as integer), cast(renterTripsTaken as integer), cast(reviewCount as integer), cast(location_city as string) as city, cast(Official_Name_State as string) as state_name, cast(owner_id as integer), cast(rate_daily as integer), cast(vehicle_make as string) as make, cast(vehicle_model as string) as model, cast(vehicle_year as integer) as year from vista_nueva")

new_df.write.mode("overwrite").saveAsTable("car_rental_db.car_rental_analytics")




