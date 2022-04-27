#Imports
import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import avg

# The virgin RDD: Ininteligible y confuso, bajo nivel

sc = SparkContext()
dataRDD = sc.parallelize([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)])
agesRDD = dataRDD.map(lambda x: (x[0], (x[1], 1))).reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0], (x[1][0]/x[1][1])))

# The chad DF: Entendible, alto nivel

spark = SparkSession.builder.appName("Ejemplo1").getOrCreate()

data_df = spark.createDataFrame([("Brooke",20),("Denny",31),("Jules",30),("TD",35),("Brooke",25)], ["name", "age"])
avg_df = data_df.groupBy("name").agg(avg("age"))
avg_df.show()