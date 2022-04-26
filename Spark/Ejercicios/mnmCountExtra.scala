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
            .agg(max("Count").alias("Max"))
            .orderBy(col("Max").desc)
    
        countMnMDF.show(60)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        spark.stop()

    }
}