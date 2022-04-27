package main.scala.chapter3.ejemplo2

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import scala.collection.JavaConversions._

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
        val schema3 = StructType(Array(
            StructField("Id", IntegerType, false),
            StructField("First", StringType, false),
            StructField("Last", StringType, false),
            StructField("Url", StringType, false),
            StructField("Published", StringType, false),
            StructField("Hits", IntegerType, false),
            StructField("Campaigns", ArrayType(StringType), false),
        ))

        // doesnt work pero bueno eso
        val data = Seq(Row(1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, Array("twitter","LinkedIn")),
        Row(2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, Array("twitter","LinkedIn")),
        Row(3, "Denny","Lee", "https://tinyurl.3", "6/7/2019", 7659, Array("web","twitter","FB","LinkedIn")),
        Row(4, "Tathagata","Das", "https://tinyurl.4", "5/12/2018", 10568, Array("twitter", "FB")),
        Row(5, "Matei","Zaharta", "https://tinyurl.5", "5/14/2014", 40578, Array("web", "twitter","FB", "LinkedIn")),
        Row(6, "Reynold","Xin", "https://tinyurl.6", "3/2/2015", 25568, Array("twitter", "LinkedIn"))
        )

        val blogs_df = spark.createDataFrame(data,schema3)

        blogs_df.show()

        blogs_df.printSchema()

    }
}