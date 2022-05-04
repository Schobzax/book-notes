from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# LECTURA

# Cargamos archivo
file = "json/*"

# Creamos DF
df = spark.read.format("json").load(file)

# Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING json
             OPTIONS (
                 path "json/*"
             )""")

# Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# ESCRITURA

# A archivo
(df.write.format("json")
    .mode("overwrite")
    .option("compression","snappy")
    .save("/tmp/data/json/df_json"))