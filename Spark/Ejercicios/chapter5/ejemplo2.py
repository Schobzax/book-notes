import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Creamos la sesión
spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

# Definición de la función
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Creación de la UDF de Pandas para la función anterior
cubed_udf = pandas_udf(cubed, returnType=LongType()) # De esta manera se declara una función llamada cubed

# Vamos a probar esta función.
x = pd.Series([1, 2, 3])
print(cubed(x))

# Creamos un DF para imprimr esto de una manera más visual
df = spark.range(1, 4)

df.select("id", cubed_udf(col("id"))).show()
