import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Creamos la sesión
val spark = SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate()

// Creamos la vista mediante SQL
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
           SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO';""")
           
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_JFK_global_tmp_view AS
           SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'""")

// Creamos la vista mediante la API DataFrame
dfSFO = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
dfJFK = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

dfSFO.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_temp_view")
dfJFK.createOrReplaceTempView("us_origin_airport_JFK_temp_view")

// Una vez creadas podemos acceder a ellas de forma normal

// Vista global
spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")

// Vista normal
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

// Borrado
spark.sql("DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view; DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")

// Borrado en API
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

// Acceso a metadatos
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")