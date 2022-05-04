from pyspark.sql import SparkSession

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Creamos la vista mediante SQL
spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
           SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO';""")
           
spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_global_tmp_view AS
           SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'""")

# Creamos la vista mediante la API DataFrame
df_sfo = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'JFK'")

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# Una vez creadas podemos acceder a ellas de forma normal

# Vista Global
spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view")

# Vista Normal
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")

# Borrado
spark.sql("DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view; DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")

# Borrado en API
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# Acceso a metadatos
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")