package main.scala.chapter3.ejemplo2

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

// Ejemplos de definición de un schema

object ejemplo {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.appName("ejemplo1").getOrCreate()
        // Definición programática
        val schema = StructType(Array(StructField("author", StringType, false),
                                      StructField("title",StringType, false),
                                      StructField("pages",IntegerType, false)))

        // Definición por DDL
        val schema2 = "author STRING, title STRING, pages INT"

        // Otra definición por DDL
        val schema3 = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"
        /*val schema3 = List(
            StructField("Id", IntegerType, false),
            StructField("First", StringType, false),
            StructField("Last", StringType, false),
            StructField("Url", StringType, false),
            StructField("Published", StringType, false),
            StructField("Hits", IntegerType, false),
            StructField("Campaigns", ArrayType(StringType,false), false),
        )
        */

        // doesnt work pero bueno eso
    /*
        val data = Seq((1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ("twitter","LinkedIn")),
        (2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ("twitter","LinkedIn")),
        (3, "Denny","Lee", "https://tinyurl.3", "6/7/2019", 7659, ("web","twitter","FB","LinkedIn")),
        (4, "Tathagata","Das", "https://tinyurl.4", "5/12/2018", 10568, ("twitter", "FB")),
        (5, "Matei","Zaharta", "https://tinyurl.5", "5/14/2014", 40578, ("web", "twitter","FB", "LinkedIn")),
        (6, "Reynold","Xin", "https://tinyurl.6", "3/2/2015", 25568, ("twitter", "LinkedIn"))
        )

        val blogs_df = spark.createDataFrame(data,schema3)

        blogs_df.show()

        blogs_df.printSchema()
    */

    }
}