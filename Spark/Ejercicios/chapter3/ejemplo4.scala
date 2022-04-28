package main.scala.chapter3.ejemplo4

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object ejemplo {
    def main(args: Array[String]) {
        // Repetimos lo del ejemplo anterior.
        val spark = SparkSession
            .builder
            .appName("Ejemplo4")
            .getOrCreate()

        import spark.implicits._

        if (args.length <= 0) {
            println("uso: Ejemplo4 <ruta a blogs.json>")
            System.exit(1)
        }

        val jsonFile = args(0)
        val schema = StructType(Array(StructField("Id",        IntegerType, false),
                                      StructField("First",     StringType,  false),
                                      StructField("Last",      StringType,  false),
                                      StructField("Url",       StringType,  false),
                                      StructField("Published", StringType,  false),
                                      StructField("Hits",      IntegerType, false),
                                      StructField("Campaigns", ArrayType(StringType), false)))
        val blogsDF = spark.read.schema(schema).json(jsonFile)

        // Algunos comandos de columnas

        // Devuelve un array de columnas
        println(blogsDF.columns)

        // Devuelve una Column particular
        println(blogsDF.col("Id"))


        // Computa un valor a partir de una columna
        blogsDF.select(expr("Hits * 2")).show(2)
        blogsDF.select(col("Hits") * 2).show(2)

        // Añadimos un valor condicional
        blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

        // Concatenación de columnas
        blogsDF.withColumn("AuthorsID", (concat(expr("First"), expr("Last"), expr("Id"))))
        .select(col("AuthorsID"))
        .show(4)

        // Distintas maneras de llamar a lo mismo.
        blogsDF.select(expr("Hits")).show(2)
        blogsDF.select(col("Hits")).show(2)
        blogsDF.select("Hits").show(2)

        // Ordenación
        blogsDF.sort(col("Id").desc).show() // Devuelve un objeto Column.
        blogsDF.sort($"Id".desc).show() // Convierte Id a Column.
    }
}