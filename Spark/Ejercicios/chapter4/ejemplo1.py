from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Tomamos el archivo csv
csv_file = "departuredelays.csv"

# Creamos un DataFrame a partir del CSV
df = (spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")