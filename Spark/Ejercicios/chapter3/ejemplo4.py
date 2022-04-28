import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("Ejemplo4").getOrCreate())

    if len(sys.argv) != 2:
        print("uso: Ejemplo4 <ruta a blogs.json>", file=sys.stderr)
        sys.exit(-1)

    jsonFile = sys.argv[1]
    schema = StructType([StructField("Id", IntegerType(), False),
                         StructField("First", StringType(), False),
                         StructField("Last", StringType(), False),
                         StructField("Url", StringType(), False),
                         StructField("Published", StringType(), False),
                         StructField("Hits", IntegerType(), False),
                         StructField("Campaigns", ArrayType(StringType()), False)
                         ])
    blogsDF = spark.read.schema(schema).json(jsonFile)

    # Devuelve las columnas
    blogsDF.columns

    # Devuelve una columna en particular
#   blogsDF.col("Id") - No funciona.
    blogsDF.columns[0]

    # Computa un valor a partir de una columna
    blogsDF.select(expr("Hits * 2")).show(2)
    blogsDF.select(col("Hits") * 2).show(2)

    # Añadimos un valor condicional
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    # Concatenación de columnas
    blogsDF.withColumn("AuthorsID", concat(expr("First"), expr("Last"), expr("Id"))).select(col("AuthorsID")).show(4)

    # Distintas maneras de llamar a lo mismo.
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)

    # Ordenación - NO FUNCIONA VAMOS A VER SINTAXIS
    #blogsDF.sort(col("Id").desc).show()
    #blogsDF.sort($"Id".desc).show()