import org.apache.spark.sql.SparkSession

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// Tomamos el archivo csv
val csvFile = "departuredelays.csv"

// Creamos el DataFrame a partir del CSV
val df = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csvFile)

// Creamos la vista temporal a partir del DF
df.createOrReplaceTempView("us_delay_flights_tbl")