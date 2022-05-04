from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# LECTURA

# Cargamos archivo
file = "avro/*"

# Creamos DF
df = (spark.read.format("avro")
    .load(file))

# Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING avro
             OPTIONS (
                 path "avro/*"
             )""")

# Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# ESCRITURA

# A archivo
(df.write.format("avro")
    .mode("overwrite")
    .save("/tmp/data/avro/df_avro"))