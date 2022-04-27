package main.scala.chapter3.ejemplo3

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

// Leer datos de un JSON

object ejemplo {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder
            .appName("Ejemplo3")
            .getOrCreate()

        if (args.length <= 0) {
            println("uso: Ejemplo3 <ruta a blogs.json>")
            System.exit(1)
        }

        // Path al JSON
        val jsonFile = args(0)

        // Definición programática del schema
        val schema = StructType(Array(StructField("Id",        IntegerType, false),
                                      StructField("First",     StringType,  false),
                                      StructField("Last",      StringType,  false),
                                      StructField("Url",       StringType,  false),
                                      StructField("Published", StringType,  false),
                                      StructField("Hits",      IntegerType, false),
                                      StructField("Campaigns", ArrayType(StringType), false)))

        // Creamos un DataFrame leyendo del JSON
        val blogsDF = spark.read.schema(schema).json(jsonFile)

        blogsDF.show(false)

        println(blogsDF.printSchema)
        println(blogsDF.schema)

    }
}