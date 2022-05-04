from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# LECTURA

# Cargamos archivo
file = "orc/*"

# Creamos DF
df = (spark.read.format("orc")
    .load(file))

# Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING orc
             OPTIONS (
                 path "orc/*"
             )""")

# Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# ESCRITURA

# A archivo
(df.write.format("orc")
    .mode("overwrite")
    .option("compression", "snappy")
    .save("/tmp/data/orc/df_orc"))