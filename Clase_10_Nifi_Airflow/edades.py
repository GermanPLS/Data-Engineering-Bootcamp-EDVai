
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import HiveContext
sc = SparkContext('local')
spark = SparkSession(sc)
hc = HiveContext(sc)


df = spark.read.option("header","True").csv("hdfs://172.17.0.2:9000/nifi/titanic.csv")

df = df.drop('SibSp', 'Parch')

df.createOrReplaceTempView("titanic_view")
new_df = spark.sql("select cast(PassengerId as int), cast(Survived as int), Pclass, Name, Sex, cast(Age as int), Ticket, cast(Fare as float), Cabin, Embarked from titanic_view")

new_df.createOrReplaceTempView("vista_edad")

n_df = spark.sql("select *, avg(Age) over (partition by Sex) as edad_promedio from vista_edad order by PassengerId")

df_final = n_df.fillna({"Cabin": 0 })
df_final.write.mode("overwrite").saveAsTable("titanic.pasajes")