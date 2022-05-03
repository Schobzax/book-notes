import sys

from pyspark.sql import Row

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Ejemplo6")
        .getOrCreate())

    row = Row(350, True, "Learning Spark 2E", None)

    print(row[0])
    print(row[1])
    print(row[2])