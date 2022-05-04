import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// LECTURA

// Cargamos archivo
val file = "avro/*"

// Creamos DF
val df = spark.read.format("avro")
    .load(file)

// Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING avro
             OPTIONS (
                 path "avro/*"
             )""")

// Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// ESCRITURA

// A archivo
df.write.format("avro")
    .mode("overwrite")
    .save("/tmp/data/avro/df_avro")