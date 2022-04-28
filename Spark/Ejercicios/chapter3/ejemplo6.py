import sys

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Ejemplo6")
        .getOrCreate())

    # Definimos el schema
    fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
        StructField('UnitID', StringType(), True),
        StructField('IncidentNumber', IntegerType(), True),
        StructField('CallType', StringType(), True),
        StructField('CallDate', StringType(), True),
        StructField('WatchDate', StringType(), True),
        StructField('CallFinalDisposition', StringType(), True),
        StructField('AvailableDtTm', StringType(), True),
        StructField('Address', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Zipcode', IntegerType(), True),
        StructField('Battalion', StringType(), True),
        StructField('StationArea', StringType(), True),
        StructField('Box', StringType(), True),
        StructField('OriginalPriority', StringType(), True),
        StructField('Priority', StringType(), True),
        StructField('FinalPriority', StringType(), True),
        StructField('ALSUnit', BooleanType(), True),
        StructField('CallTypeGroup', StringType(), True),
        StructField('NumAlarms', IntegerType(), True),
        StructField('UnitType', StringType(), True),
        StructField('UnitSequenceInCallDispatch', IntegerType(), True),
        StructField('FirePreventionDistrict', StringType(), True),
        StructField('SupervisorDistrict', StringType(), True),
        StructField('Neighborhood', StringType(), True),
        StructField('Location', StringType(), True),
        StructField('RowID', StringType(), True),
        StructField('Delay', FloatType(), True)])

    # Cargamos el CSV y creamos el DF
    sf_fire_file = "sf-fire-calls.csv"
    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# Parte 2?
    ## Ejemplo de proyecciones y filtros
    few_fire_df = (fire_df
        .filter(col("CallType") != "Medical Incident")
        .select("IncidentNumber", "AvailableDtTm", "CallType"))
    
    few_fire_df.show(5, truncate=False)

    # Número de tipos de llamada (CallTypes) distintos como causa de las llamadas

    (fire_df
    .select("CallType")
    .filter(col("CallType").isNotNull())
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    .show())

    # Lista de tipos de llamada (CallTypes) distintos

    (fire_df
    .select("CallType")
    .filter(col("CallType").isNotNull())
    .distinct()
    .show(10, False))

# Parte 3?
    # Ejemplo de renombrado de columnas

    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedInMins")
    (new_fire_df
        .select("ResponseDelayedinMins")
        .where(col("ResponseDelayedinMins") > 5)
        .show(5, False))

    # Casting
    fire_ts_df = (new_fire_df
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "dd/MM/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "dd/MM/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "dd/MM/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm"))

    (fire_ts_df
        .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
        .show(5, False))

    # Operaciones de Timestamp

    (fire_ts_df
        .select(year("IncidentDate"))
        .distinct()
        .orderBy(year("IncidentDate"))
        .show())

# Parte 4: Agregaciones

    # Tipos de llamadas más comunes

    (fire_ts_df
        .select("CallType")
        .where(col("CallType").isNotNull())
        .groupBy("CallType")
        .count()
        .orderBy("count", ascending=False)
        .show(n=10, truncate=False))

    # Suma de alarmas, tiempo de respuesta medio, mínimo y máximo.

    (fire_ts_df
        .select(sum("NumAlarms"), avg("ResponseDelayinMins"), min("ResponseDelayinMins"), max("ResponseDelayinMins"))
        .show())

# Parte 5: Ejercicios adicionales

    # What were all the different types of fire calls in 2018?
    # Distintos tipos de llamadas para el año 2018

    (fire_ts_df
        .filter(year("IncidentDate") == 2018)
        .select(col("CallType"))
        .distinct()).show()

    # What months within the year 2018 saw the highest number of fire calls?
    # Meses del año 2018 con el mayor número de llamadas

    (fire_ts_df 
        .filter(year("IncidentDate") == 2018)
        .withColumn("IncidentMonth", month("IncidentDate"))
        .withColumn("IncidentYear", year("IncidentDate"))
        .groupBy("IncidentYear", "IncidentMonth")
        .agg(count("*").alias("Count"))
        .orderBy("IncidentMonth")).show()

    # Which neighborhood in San Francisco generated the most fire calls in 2018?
    # Barrio con más llamadas en 2018

    (fire_ts_df
        .filter(year("IncidentDate") == 2018)
        .withColumn("Neighborhood", col("Neighborhood"))
        .groupBy("Neighborhood")
        .agg(count("*").alias("Count"))
        .orderBy(desc("Count"))
        .distinct()).display()

    # Which neighborhoods had the worst response times to fire calls in 2018?
    # Barrios con el peor tiempo de respuesta en 2018

    (fire_ts_df
        .filter(year("IncidentDate") == 2018)
        .withColumn("Neighborhood", col("Neighborhood"))
        .groupBy("Neighborhood")
        .agg(avg("ResponseDelayInMins").alias("AvgResponseTime"))
        .orderBy(desc("AvgResponseTime")))

    # Which week in the year in 2018 had the most fire calls?
    # Semana del año con más llamadas

    (fire_ts_df
        .filter(year("IncidentDate") == 2018)
        .withColumn("Semana", weekofyear("IncidentDate"))
        .distinct()
        .groupBy("Semana")
        .agg(count("*").alias("Count"))
        .orderBy("Semana"))

    # Is there a correlation between neighborhood, zip code, and number of fire calls?
    # Correlación entre barrio, código postal, y número de llamadas

    # Respuesta:
    # A ver. Evidentemente entre barrio y código postal hay. Entre eso y el número de llamadas... Pues no parece.
    # Evidentemente si en un barrio hay más número de llamadas, en un código postal también.

    # How can we use Parquet files or SQL tables to store this data and read it back?
    # Guardar en archivo Parquet o tabla SQL y leerlo de vuelta

    # Parquet
    fire_ts_df.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")
    file_parquet_df = spark.read.format("parquet").load("/tmp/fireServiceParquet/")
    
    # SQL
    fire_ts_df.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")
    file_sql_df = spark.read.load("FireServiceParquet")