package main.scala.chapter3.esquemacsv

object ejercicio {
    def main(args: Array[String]) {
        val mnmDF = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load("mnm_dataset.csv")

        val mnmDF_schema = mnmDF.schema // Este es el schema que se obtiene.

        // Ahora vamos a proceder al guardado en distintos formatos

        mnmDF.write.format("json").mode("overwrite").save("new-mnm-dataset.json")
        mnmDF.write.format("csv").mode("overwrite").save("new-mnm-dataset.csv")
        mnmDF.write.format("avro").mode("overwrite").save("new-mnm-dataset.avro")
    }
}