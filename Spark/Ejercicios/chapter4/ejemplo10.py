from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Cargamos archivo
file = "cctvVideos/train_images/*"

# Creamos DF
df = (spark.read.format("image")
    .load(file))

# Mostramos el schema
df.printSchema()

# Mostramos algunos datos
df.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show()