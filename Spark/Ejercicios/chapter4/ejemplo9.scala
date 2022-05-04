import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// LECTURA

// Cargamos archivo
val file = "orc/*"

// Creamos DF
val df = spark.read.format("orc")
    .load(file)

// Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING orc
             OPTIONS (
                 path "orc/*"
             )""")

// Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// ESCRITURA

// A archivo
df.write.format("orc")
    .mode("overwrite")
    .option("compression","snappy")
    .save("/tmp/data/orc/df_orc")