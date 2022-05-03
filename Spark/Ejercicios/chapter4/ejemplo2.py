from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("departuredelays.csv"))

# 1. Vuelos cuya distancia es mayor a 1000 millas

# SQL
spark.sql("""SELECT DISTINCT distance, origin, destination FROM us_delay_flights_tbl
              WHERE distance > 1000
              ORDER BY distance DESC""").show()

# DataFrame
(df.filter(col("distance") > 1000)
     .select("distance", "origin", "destination")
    .withColumn("distance", col("distance"))
    .withColumn("origin", col("origin"))
    .withColumn("destination", col("destination"))
     .distinct()
    .sort(col("distance").desc())).show()

# 2. Vuelos entre San Francisco y Chicago con al menos 2 horas de retraso

# SQL
spark.sql("""SELECT delay, distance, origin, destination FROM us_delay_flights_tbl
             WHERE origin LIKE 'SFO' AND destination LIKE 'ORD' AND delay > 120
             ORDER BY delay DESC
            """).show()

# DataFrame
(df.filter(
    ((col("origin") == "SFO") & (col("destination") == "ORD"))
    & (col("delay") >= 120)
).select("delay","distance","origin","destination")
 .sort(col("delay").desc())).show()

 # 3. Marcar con texto los retrasos por grupo

 # SQL
 spark.sql("""SELECT delay, origin, destination,
             CASE
                 WHEN delay >= 360 THEN 'Very Long Delay'
                 WHEN delay >= 120 AND delay < 360 THEN 'Long Delay'
                 WHEN delay >= 60 AND delay < 120 THEN 'Short Delay'
                 WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delay'
                 WHEN delay == 0 THEN 'No Delay'
                 ELSE 'Early'
             END AS Flight_Delays
             FROM us_delay_flights_tbl
             ORDER BY origin, delay DESC""").show()

# DataFrame
(df.withColumn("Flight_Delays", when((df.delay >= 360), "Very Long Delay")
                               .when((df.delay >= 120) & (df.delay < 360), "Long Delay")
                               .when((df.delay >= 60) & (df.delay < 120), "Short Delay")
                               .when((df.delay > 0) & (df.delay < 60), "Tolerable Delay")
                               .when(df.delay == 0, "No Delay")
                               .otherwise("Early"))
    .select("delay", "origin", "destination", "Flight_Delays")
    .sort("origin", col("delay").desc())).show()