package main.scala.chapter3.ejemplo5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object ejemplo {
    def main(args: Array[String]) {

        val spark = SparkSession
            .builder
            .appName("Ejemplo5")
            .getOrCreate()

        import spark.implicits._

        val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array("twitter", "LinkedIn"))

        println(blogRow(1))

        val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
        val authorsDF = rows.toDF("Author", "State")
        authorsDF.show()
    }
}