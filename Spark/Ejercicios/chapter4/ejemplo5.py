from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# LECTURA

# Cargamos archivo
file = "2010-summary-parquet"

# Creamos DF
df = spark.read.format("parquet").load(file)

# Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING parquet
             OPTIONS (
                 path "2010-summary-parquet"
             )""")

# Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# ESCRITURA

# A archivo
(df.write.format("parquet")
    .mode("overwrite")
    .option("compression","snappy")
    .save("/tmp/data/parquet/df_parquet"))

# A tabla
(df.write
    .mode("overwrite")
    .saveAsTable("us_delay_flights_tbl"))