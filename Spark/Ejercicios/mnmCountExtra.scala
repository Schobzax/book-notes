package main.scala.chapter2.extra.mnm

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCountExtra {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("MnMCountExtra")
            .getOrCreate()
        
        if (args.length > 1) {
            print("Usage: MnMCount <mnm_file_dataset>")
            sys.exit(1)
        }

        val mnmFile = args(0) // O la ruta

        val mnmDF = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(mnmFile)

        val countMnMDF = mnmDF
            .select("State","Color","Count")
            .groupBy("State","Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        countMnMDF.show(60)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        val caCountMnMDF = mnmDF
            .select("State","Color","Count")
            .where(col("State") === "CA")
            .groupBy("State","Color")
            .agg(count("Count").alias("Total"))
            .orderBy(desc("Total"))

        caCountMnMDF.show(10)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        val maxMnMDF = mnmDF 
            .select("State","Color","Count")
            .groupBy("State","Color")
            .agg(max("Count").alias("Max"))
            .orderBy(col("Max").desc)
    
        maxMnMDF.show(60)
        println(s"Total Rows = ${maxMnMDF.count()}")
        println()

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
    }
}