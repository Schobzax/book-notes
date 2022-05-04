import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

// Lectura en formato Avro
val df_avro = (spark.read.format("avro").load("dbfs:/new-mnm-dataset.avro/*"))
df_avro.show()

// Lectura en formato CSV
val df_csv = (spark.read.format("csv").load("dbfs:/new-mnm-dataset.csv/*"))
df_csv.show()

// Lectura en formato JSON
val df_json = (spark.read.format("json").load("dbfs:/new-mnm-dataset.json/*"))
df_json.show()