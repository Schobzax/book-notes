import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

// Cargamos archivo
val path = "cctv/train_images/"
val df = (spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg") // Permite filtrar el tipo de archivo que se va a guardar.
    .load(path))

df.show()

// Para ignorar el particionado de data discovery, recursiveFile = true.
val df2 = spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .option("recursiveFileLookup", "true")
    .load(path)

df.show()