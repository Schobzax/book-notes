package main.scala.chapter3.ejemplo1

// Imports
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ejemplo {
    def main(args: Array[String]) {
        val sc = SparkContext.getOrCreate();
        // The virgin RDD: Ininteligible y confuso, bajo nivel

        val dataRDD = sc.parallelize(Array(("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)))
        val agesRDD = dataRDD.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1+y._1,x._2+y._2)).map(x => (x._1, (x._2._1/x._2._2)))

        agesRDD.take(5) // res10: Array[(String, Int)] = Array((Denny,31), (TD,35), (Brooke,22.5), (Jules,30))

        // The chad DF: Entendible, alto nivel
        
        // Creamos la SparkSession
        val spark = SparkSession.builder.appName("ejemplo1").getOrCreate()

        val data_df = spark.createDataFrame(Seq(("Brooke",20), ("Brooke", 25), ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name","age")
        data_df.printSchema()
        val avg_df = data_df.groupBy("name").agg(avg("age"))
        avg_df.show() // Muestra una tabla muy bonica con los mismos datos de arriba
    }
}