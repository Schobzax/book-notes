from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Creamos una base de datos y la usamos
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# 1. Creación de tabla managed

# Creamos la tabla mediante SQL
spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

# Creamos la tabla mediante DataFrame API
csv_file = "departuredelays.csv"

schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# 2. Creación de tabla unmanaged

# Mediante SQL
spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT, distance INT, origin STRING, destination STRING)
             USING csv OPTIONS (PATH 'departuredelays.csv')""")

# Mediante DataFrame API
(flights_df.write.option("path", "/tmp/data/us_flights_delay").saveAsTable("us_delay_flights_tbl"))