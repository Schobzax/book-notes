// Imports
package main.scala.chapter2.extra.quijote

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QuijoteCount {
    def main(args: Array[String]) {
        // Creamos el contexto
        val spark = SparkSession
            .builder
            .appName("quijoteCount")
            .getOrCreate()

        // Comprobamos la ejecución
        if (args.length < 1) {
            print("Usage: quijoteCount <archivo_quijote>")
            sys.exit(1)
        }

        // Tomamos el archivo desde los argumentos de consola
        val quijoteFile = args(0)

        val quijoteDF = spark.read.text(quijoteFile)

        quijoteDF.show() // Muestra por defecto 20 líneas con una longitud de 20? (17 con puntos suspensivos) caracteres.

        val quijoteConteo = quijoteDF.count()
        println(s"Líneas: $quijoteConteo") // Imprime el número de líneas

        // Distintas opciones para show.
        quijoteDF.show(false) // Muestra 20 líneas (por defecto) en longitud completa.
        quijoteDF.show(10) // Muestra solo las 10 primeras líneas.
        quijoteDF.show(5,100,true) // Muestra las 5 primeras líneas cortando a longitud 100 en formato vertical.
        // Aquí cambia respecto a Python: el orden ESTRICTO es nº de líneas, truncate (boolean o número de carácteres) y verticalidad. No puede especificarse.

        val quijoteHead = quijoteDF.head(5) // Devuelve las primeras 5 filas como una lista de Row.
        val quijoteTake = quijoteDF.take(5) // ídem.
        val quijoteFirst = quijoteDF.first() // Devuelve la primera fila como una Row.

        // Pero ojo, porque...
        val quijoteHeadEmpty = quijoteDF.head() // Devuelve la primera línea como una Row.
        val quijoteFirstEmpty = quijoteDF.take() // Error: Requiere un argumento porque siempre devuelve un array.
        // val quijoteFirstEmpty = quijoteDF.take(1) // No da error, devuelve un Array de Rows de tamaño 1.

        spark.stop()
    }
}