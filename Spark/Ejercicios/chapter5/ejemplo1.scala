import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

val cubed = (s: Long) => { s * s * s }

spark.udf.register("cubed", cubed)

spark.range(1,9).createOrReplaceTempView("udf_test")

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()