import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// LECTURA

// Cargamos archivo
val file = "json/*"

// Creamos DF
val df = spark.read.format("json").load(file)

// Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING json
             OPTIONS (
                 path "json/*"
             )""")

// Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// ESCRITURA

// A archivo
df.write.format("json")
    .mode("overwrite")
    .option("compression","snappy")
    .save("/tmp/data/json/df_json")
