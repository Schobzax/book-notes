import sys

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName("Ejemplo6")
        .getOrCreate())

    mnm_df = (spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load("mnm_dataset.csv"))

    mnm_df_schema = mnm_df.schema # Este es el schema que se obtiene.

    # Ahora vamos a proceder al guardado en distintos formatos

    mnm_df.write.format("json").mode("overwrite").save("new-mnm-dataset.json")
    mnm_df.write.format("csv").mode("overwrite").save("new-mnm-dataset.csv")
    mnm_df.write.format("avro").mode("overwrite").save("new-mnm-dataset.avro")