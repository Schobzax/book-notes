import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// LECTURA

// Cargamos archivo
val file = "csv/*"
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT" // En este caso sí es necesario agregar el schema, para agilizar la carga.

// Creamos DF
val df = spark.read.format("csv")
    .schema(schema)
    .option("header","true")
    .option("mode","FAILFAST") // Exit if any errors
    .option("nullValue", "") // Replace any null data with quotes
    .load(file)

// Lectura a tabla
spark.sql("""CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
             USING csv
             OPTIONS (
                 path "csv/*",
                 header "true",
                 inferSchema "true",
                 mode "FAILFAST"
             )""")

// Lectura desde esa misma tabla
spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// ESCRITURA

// A archivo
df.write.format("csv")
    .mode("overwrite")
    .save("/tmp/data/csv/df_csv")