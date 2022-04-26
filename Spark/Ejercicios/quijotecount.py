import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 1:
            print("Usage: quijotecount, sin argumentos", file=sys.stderr)
            sys.exit(-1)

    print(sys.getdefaultencoding())
    print(sys.getfilesystemencoding())

    spark = (SparkSession
        .builder
        .appName("quijotecount")
        .getOrCreate())
    quijotefile = "el_quijote.txt"
    quijoteDF = spark.read.text(quijotefile)
    quijoteDF.show()

    println("Líneas: %d" % (quijoteDF.count()))

    spark.stop()