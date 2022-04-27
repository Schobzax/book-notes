# Imports
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Comprobamos la ejecución
if __name__ == "__main__":
    if len(sys.argv) != 1:
            print("Usage: quijotecount, sin argumentos", file=sys.stderr)
            sys.exit(-1)

# Creamos la SparkSession
    spark = (SparkSession
        .builder
        .appName("quijotecount")
        .getOrCreate())
    quijotefile = "el_quijote.txt"
    quijoteDF = spark.read.text(quijotefile)
    quijoteDF.show() # show por defecto muestra 20 líneas, con una longitud de ¿20? (17 con puntos suspensivos) carácteres.

    println("Líneas: %d" % (quijoteDF.count())) # Imprime el número de líneas totales.

    # Distintas opciones para show.
    quijoteDF.show(truncate=10) # Muestra 20 líneas (por defecto) cortando en longitud 10.
    quijoteDF.show(10) # Muestra solo las 10 primeras líneas
    quijoteDF.show(vertical=True) # Muestra la salida en formato vertical (dividido por rows)
    quijoteDF.show(5,truncate=100,vertical=True) # Muestra las 5 primeras líneas cortando a longitud 100 en formato vertical.

    quijoteHead = quijoteDF.head(5) # Devuelve las primeras 5 filas como una lista de Row.
    for linea in quijoteHead:
        print(linea)
    quijoteTake = quijoteDF.take(5) # Devuelve las primeras 5 filas como una lista de Row.
    for linea in quijoteTake:
        print(linea)
    quijoteDF.first() # Devuelve la primera fila como una Row.

    # Pero ojito, porque...
    # quijoteDF.head() # Devuelve la primera línea como una Row.
    # quijoteDF.take() # Da error: requiere un argumento porque devuelve siempre un Array.
    # quijoteDF.take(1) # No da error pero eso, devuelve un array de rows de tamaño 1.

    spark.stop()