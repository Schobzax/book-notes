package main.scala.chapter3.ejemplo6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ejemplo {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Ejemplo6")
            .getOrCreate()
        
        import spark.implicits._

        // Definimos el schema
        val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
            StructField("UnitID", StringType, true),
            StructField("IncidentNumber", IntegerType, true),
            StructField("CallType", StringType, true),
            StructField("CallDate", StringType, true),
            StructField("WatchDate", StringType, true),
            StructField("CallFinalDisposition", StringType, true),
            StructField("AvailableDtTm", StringType, true),
            StructField("Address", StringType, true),
            StructField("City", StringType, true),
            StructField("Zipcode", StringType, true),
            StructField("Battalion", StringType, true),
            StructField("StationArea", StringType, true),
            StructField("Box", StringType, true),
            StructField("OriginalPriority", StringType, true),
            StructField("Priority", StringType, true),
            StructField("FinalPriority", StringType, true),
            StructField("ALSUnit", StringType, true),
            StructField("CallTypeGroup", StringType, true),
            StructField("NumAlarms", IntegerType, true),
            StructField("UnitType", StringType, true),
            StructField("UnitSequenceInCallDispatch", IntegerType, true),
            StructField("FirePreventionDistrict", StringType, true),
            StructField("SupervisorDistrict", StringType, true),
            StructField("Neighborhood", StringType, true),
            StructField("Location", StringType, true),
            StructField("RowID", StringType, true),
            StructField("Delay", FloatType, true)
        ))

        // Cargamos el CSV y creamos el DF
        val sfFireFile = "sf-fire-calls.csv"
        val fireDF = spark.read.schema(fireSchema)
            .option("header", "true")
            .csv(sfFireFile)

    // Parte 2
        // Ejemplo de proyecciones y filtros
        val fewFireDF = (fireDF
            .filter($"CallType" =!= "Medical Incident")
            .select("IncidentNumber", "AvailableDtTm", "CallType"))

        fewFireDF.show(5, false)

        // Número de tipos de llamada (CallTypes) distintos como causa de las llamadas
        fireDF
            .select("CallType")
            .filter(col("CallType").isNotNull)
            .agg(countDistinct("CallType") as "DistinctCallTypes")
            .show()

        // Lista de tipos de llamada (CallTypes) distintos
        fireDF
            .select("CallType")
            .filter(col("CallType").isNotNull)
            .distinct()
            .show(10, false)

    // Parte 3?
        // Ejemplo de renombrado de columnas

        val newFireDF = fireDF.withColumnRenamed("Delay","ResponseDelayedinMins")
        newFireDF
            .select("ResponseDelayedinMins")
            .where($"ResponseDelayedinMins" > 5)
            .show(5, false)

        // Casting
        val fireTsDF = newFireDF
            .withColumn("IncidentDate", to_timestamp(col("CallDate"), "dd/MM/yyyy"))
            .drop("CallDate")
            .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "dd/MM/yyyy"))
            .drop("WatchDate")
            .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"dd/MM/yyyy hh:mm:ss a"))
            .drop("AvailableDtTm")

        fireTsDF
            .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
            .show(5, false)

        // Funciones de Timestamp
        fireTsDF
            .select(year($"IncidentDate"))
            .distinct()
            .orderBy(year($"IncidentDate"))
            .show()

    // Parte 4: Agregaciones

        // Llamadas más comunes

        fireTsDF
            .select("CallType")
            .where(col("CallType").isNotNull)
            .groupBy("CallType")
            .count()
            .orderBy(desc("count"))
            .show(10, false)

        // Suma de alarmas, tiempo de respuesta medio, mínimo y máximo.

        fireTsDF
            .withColumn("SumAlarms", sum("NumAlarms"))
            .withColumn("AvgResponse", avg("ResponseDelayedinMins"))
            .withColumn("MinResponse", min("ResponseDelayedinMins"))
            .withColumn("MaxResponse", max("ResponseDelayedinMins"))
            .show()

    // Parte 5: Ejercicios adicionales

        // What were all the different types of fire calls in 2018?
        // Distintos tipos de llamadas para el año 2018

        fireTsDF
            .filter(year($"IncidentDate") === 2018)
            .select(col("CallType"))
            .distinct().show()

        // What months within the year 2018 saw the highest number of fire calls?
        // Meses del año 2018 con el mayor número de llamadas

        fireTsDF
            .filter(year($"IncidentDate") === 2018)
            .withColumn("IncidentMonth", month($"IncidentDate"))
            .withColumn("IncidentYear", year($"IncidentDate"))
            .groupBy($"IncidentYear", $"IncidentMonth")
            .agg(count("*").alias("Count"))
            .orderBy($"IncidentMonth").show()

        // Which neighborhood in San Francisco generated the most fire calls in 2018?
        // Barrio con más llamadas en 2018

        fireTsDF
            .filter(year($"IncidentDate") === 2018)
            .select("Neighborhood")
            .groupBy("Neighborhood")
            .agg(count("*").alias("Count"))
            .orderBy(desc("Count"))
            .distinct()

        // Which neighborhoods had the worst response times to fire calls in 2018?
        // Barrios con el peor tiempo de respuesta en 2018

        fireTsDF
            .filter(year($"IncidentDate") === 2018)
            .withColumn("Neighborhood", col("Neighborhood"))
            .groupBy("Neighborhood")
            .agg(avg($"ResponseDelayInMins").alias("AvgResponseTime"))
            .orderBy(desc("AvgResponseTime"))

        // Which week in the year in 2018 had the most fire calls?
        // Semana del año con más llamadas
        fireTsDF
            .filter(year($"IncidentDate") === 2018)
            .withColumn("Semana", weekofyear($"IncidentDate"))
            .distinct()
            .groupBy("Semana")
            .agg(count("*").alias("Count"))
            .orderBy("Semana")

        // Is there a correlation between neighborhood, zip code, and number of fire calls?
        // Correlación entre barrio, código postal, y número de llamadas

        // Respuesta:
        // A ver. Evidentemente entre barrio y código postal hay. Entre eso y el número de llamadas... Pues no parece.
        // Evidentemente si en un barrio hay más número de llamadas, en un código postal también.

        // How can we use Parquet files or SQL tables to store this data and read it back?
        // Guardar en archivo Parquet o tabla SQL y leerlo de vuelta

        // Parquet
        fireTsDF.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")
        val fileParquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")
        
        // SQL
        fireTsDF.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")
        //val fileSqlDF = spark.read.load("FireServiceParquet") --No es así, se accede directamente al SQL

    }
}