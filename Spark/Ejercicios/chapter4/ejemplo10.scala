import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

// Cargamos archivo
val file = "cctvVideos/train_images/*"

// Creamos DF
val df = spark.read.format("image")
    .load(file)

// Mostramos el schema
df.printSchema

// Mostramos algunos datos
df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show()