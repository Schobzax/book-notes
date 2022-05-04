from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Lectura en formato Avro
df_avro = (spark.read.format("avro").load("dbfs:/new-mnm-dataset.avro/*"))
df_avro.show()

# Lectura en formato CSV
df_csv = (spark.read.format("csv").load("dbfs:/new-mnm-dataset.csv/*"))
df_csv.show()

# Lectura en formato JSON
df_json = (spark.read.format("json").load("dbfs:/new-mnm-dataset.json/*"))
df_json.display()