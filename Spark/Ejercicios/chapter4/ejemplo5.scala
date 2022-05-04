import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// LECTURA

// Cargamos archivo
val file = "2010-summary-parquet"

// Creamos DF
val df = spark.read.format("parquet").load(file)

// Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING parquet
             OPTIONS (
                 path "2010-summary-parquet"
             )""")

// Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// ESCRITURA

// A archivo
df.write.format("parquet")
    .mode("overwrite")
    .option("compression","snappy")
    .save("/tmp/data/parquet/df_parquet")

// A tabla
(df.write
    .mode("overwrite")
    .saveAsTable("us_delay_flights_tbl"))