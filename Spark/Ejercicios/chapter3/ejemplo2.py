import sys

from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Ejemplos de definición de un schema

# Definición programática
schema = StructType([StructField("author", StringType(), False),
                     StructField("title", StringType(), False),
                     StructField("pages", StringType(), False)])

# Definición por DDL
schema2 = "author STRING, title STRING, pages INT"

# Otra definición por DDL
schema3 = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
        [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter","LinkedIn"]],
        [3, "Denny","Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web","twitter","FB","LinkedIn"]],
        [4, "Tathagata","Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
        [5, "Matei","Zaharta", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter","FB", "LinkedIn"]],
        [6, "Reynold","Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
       ]

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Example2")
        .getOrCreate())
    
    # Creación de un DF a partir de un schema y datos
    blogs_df = spark.createDataFrame(data,schema3)
    blogs_df.show()
    print(blogs_df.printSchema())