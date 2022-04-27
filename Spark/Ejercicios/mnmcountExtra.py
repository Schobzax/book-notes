# Imports
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    # Comprobamos el número de argumentos
    if len(sys.argv) != 2:
            print("Usage: mnmcount <file>", file=sys.stderr)
            sys.exit(-1)

    # Construimos una SparkSession
    spark = (SparkSession
        .builder
        .appName("PythonMnMCountExtra")
        .getOrCreate())

    # Leemos y creamos el DataFrame
    mnm_file = sys.argv[1]
    mnm_df = (spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(mnm_file))

    # Conteo de todos los M&Ms agrupados por Estado y Color

    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .groupBy("State","Color")
        .agg(F.count("Count").alias("Total"))
        .orderBy("Total", ascending=False))
    
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Conteo de todos los M&Ms agrupados por Estado y Color
    # Siendo el Estado = California

    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .where(mnm_df.State == "CA")
        .groupBy("State","Color")
        .agg(F.count("Count").alias("Total"))
        .orderBy("Total", ascending=False))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Nº máximo de M&Ms por Estado y Color

    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .groupBy("State","Color")
        .agg(F.max("Count").alias("Max"))
        .orderBy("Max",ascending=True))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Conteo de M&Ms por Estado y Color
    # Siendo Estado = California, Texas, Nevada o Colorado
    
    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .where(mnm_df.State.isin("CA","TX","NV","CO"))
        .groupBy("State","Color")
        .agg(F.count("Count").alias("Total"))
        .orderBy("Total", ascending=False))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Listado de funciones agregadas.

    count_mnm_df = (mnm_df
        .select("State","Color","Count")
        .groupBy("State","Color")
        .agg(F.max("Count").alias("Max"),F.avg("Count").alias("Avg"),F.min("Count").alias("Min"),F.sum("Count").alias("Sum"),F.count("Count").alias("Total"))
        .orderBy("Sum", ascending=False))

    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Prueba sencilla con SQL TempView

    count_mnm_df.createOrReplaceTempView("mnm_agg")
    tempdf = spark.sql("SELECT * FROM mnm_agg WHERE State IN ('CA','OR','WA')")
    tempdf.show(n=60, truncate=False)
    print("Total Rows = %d" % (tempdf.count()))

    spark.stop()

# Para cada ejercicio realizamos la consulta correspondiente
# Mostramos las filas necesarias
# E imprimimos el número de filas