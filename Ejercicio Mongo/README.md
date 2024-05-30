pyspark --conf "spark.mongodb.read.connection.uri=mongodb://mongoadmin:edvai@172.17.0.3:27017/?authSource=admin&authMechanism=SCRAM-SHA-1" --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1



# LEO la base de datos que creamos en MongoDB
df = spark.read.format("mongodb").option("database", "logs").option("collection", "users").load()