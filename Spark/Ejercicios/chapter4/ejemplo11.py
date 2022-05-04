from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Cargamos archivo
path = "cctv/train_images/"
df = (spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg") # Permite filtrar el tipo de archivo que se va a guardar.
    .load(path))

df.show()

# Para ignorar el particionado de data discovery, recursiveFile = true.
df = (spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.jpg")
    .option("recursiveFileLookup", "true")
    .load(path))

df.show()