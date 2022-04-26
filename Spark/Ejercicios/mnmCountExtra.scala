package main.scala.chapter2.extra.mnm

// Imports
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCountExtra {
    def main(args: Array[String]) {
        // Construimos la SparkSession
        val spark = SparkSession
            .builder
            .appName("MnMCountExtra")
            .getOrCreate()
        
        // Comprobamos el número de argumentos
        if (args.length > 1) {
            print("Usage: MnMCount <mnm_file_dataset>")
            sys.exit(1)
        }

        val mnmFile = args(0)

        // Creamos el DataFrame
        val mnmDF = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(mnmFile)

        // Conteo de todos los M&Ms agrupados por Estado y Color

        val countMnMDF = mnmDF
            .select("State","Color","Count")
            .groupBy("State","Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        countMnMDF.show(60)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        // Conteo de todos los M&Ms agrupados por Estado y Color
        // Siendo el Estado = California
        
        val caCountMnMDF = mnmDF
            .select("State","Color","Count")
            .where(col("State") === "CA")
            .groupBy("State","Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        caCountMnMDF.show(10)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        // Nº máximo de M&Ms por Estado y Color

        val maxMnMDF = mnmDF 
            .select("State","Color","Count")
            .groupBy("State","Color")
            .agg(max("Count").alias("Max"))
            .orderBy(col("Max").desc)
    
        maxMnMDF.show(60)
        println(s"Total Rows = ${maxMnMDF.count()}")
        println()

        // Conteo de M&Ms por Estado y Color
        // Siendo Estado = California, Texas, Nevada o Colorado
        
        val variosCountMnMDF = mnmDF
            .select("State","Color","Count")
            .where(col("State").isin("CA","TX","NV","CO"))
            .groupBy("State","Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        variosCountMnMDF.show(60)
        println(s"Total Rows = ${variosCountMnMDF.count()}")
        println()

        spark.stop()

    // Para cada ejercicio realizamos la consulta correspondiente
    // Mostramos las filas necesarias
    // E imprimimos el número de filas
    }
}