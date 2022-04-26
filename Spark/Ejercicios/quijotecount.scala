package main.scala.chapter2.extra.quijote

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuijoteCount {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("quijoteCount")
            .getOrCreate()

        if (args.length < 1) {
            print("Usage: quijoteCount <archivo_quijote>")
            sys.exit(1)
        }

        val quijoteFile = args(0)

        val quijoteDF = spark.read.text(quijoteFile)

        quijoteDF.show()

        val quijoteConteo = quijoteDF.count()

        println(s"Líneas: $quijoteConteo")
    }
}