import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = (SparkSession.builder.appName("Ejemplo3").getOrCreate())

    if len(sys.argv) != 2:
        print("uso: Ejemplo3 <ruta a blogs.json>", file=sys.stderr)
        sys.exit(-1)

    jsonFile = sys.argv[1]
    print(jsonFile)

    schema = StructType([StructField("Id", IntegerType(), False),
                         StructField("First", StringType(), False),
                         StructField("Last", StringType(), False),
                         StructField("Url", StringType(), False),
                         StructField("Published", StringType(), False),
                         StructField("Hits", IntegerType(), False),
                         StructField("Campaigns", ArrayType(StringType()), False)
                         ])

    blogsDF = spark.read.schema(schema).json(jsonFile)

    blogsDF.show()

    blogsDF.printSchema()
    print(blogsDF.schema)