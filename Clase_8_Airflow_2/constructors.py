constructors_df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/constructors.csv")
races_df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/races.csv")
results_df = spark.read.option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/results.csv")


constructors_df.createOrReplaceTempView("constructors")
races_df.createOrReplaceTempView("races")
results_df.createOrReplaceTempView("results")

SpanishGP = spark.sql("""
    SELECT 
        CAST(c.constructorId AS STRING) AS constructorref, 
        CAST(c.name AS STRING) AS cons_name, 
        CAST(c.nationality AS STRING) AS cons_nationality, 
        CAST(c.url AS STRING) AS url,
        CAST(r.points AS DOUBLE) AS points
    FROM constructors c
    INNER JOIN results r ON c.constructorId = r.constructorId
    INNER JOIN races ra ON ra.raceId = r.raceId
    WHERE ra.circuitId IN (4, 12, 26, 45, 49, 67) AND r.points != 0 AND ra.year = 1991
""")


SpanishGP.write.mode("overwrite").saveAsTable("formula1.constructor_results")
