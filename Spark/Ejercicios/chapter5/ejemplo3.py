from pyspark.sql.functions import expr

# Set file paths
tripDelaysFilePath = "departuredelays.csv"
airportsNaFilePath = "airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
    .format("csv")
    .options(header="true", inferSchema="true", sep="\t")
    .load(airportsNaFilePath))

airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
    .format("csv")
    .options(header="true")
    .load(tripDelaysFilePath))

departureDelays = (departureDelays
                    .withColumn("delay", expr("CAST(delay as INT) as delay"))
                    .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = (departureDelays
    .filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0""")))

foo.createOrReplaceTempView("foo")

# Muestra
spark.sql("SELECT * FROM airports_na LIMIT 10").show()
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
spark.sql("SELECT * FROM foo").show()

# 2. Uniones
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# Mostrar la unión
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()

# 3. Joins

foo.join(
    airports,
    airports.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# 4. Windowing

# Creamos una vista
spark.sql("""DROP TABLE IF EXISTS departureDelaysWindow""")
spark.sql("""CREATE TABLE departureDelaysWindow AS
             SELECT origin, destination, SUM(delay) AS TotalDelays
             FROM departureDelays
             WHERE origin IN ('SEA','SFO','JFK')
               AND destination IN ('SEA','SFO','JFK','DEN','ORD','LAX','ATL')
             GROUP BY origin, destination""").show()

# Buscamos los tres destinos con más retrasos para estos aeropuertos
spark.sql("""SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
             FROM departureDelaysWindow
             WHERE origin = '[ORIGIN]'
             GROUP BY origin, destination
             ORDER BY SUM(TotalDelays) DESC
             LIMIT 3""").show()

# Usamos dense_rank() para hacer lo mismo
spark.sql("""SELECT origin, destination, TotalDelays, rank
             FROM (
                 SELECT origin, destination, TotalDelays, dense_rank()
                   OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
                   FROM departureDelaysWindow
             ) t
             WHERE RANK <= 3""").show()

## 5. Modificaciones

foo.show()

# Añadir columna
foo2 = foo.withColumn("status",expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))
foo2.show()

# Borrar columna
foo3 = foo2.drop("delay")
foo3.show()

# Renombrar columna
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# Pivotar columna
spark.sql("""
SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
  FROM departureDelays
 WHERE origin = 'SEA'
""")

spark.sql("""
SELECT * FROM (
    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
      FROM departureDelays WHERE origin = 'SEA'
)
PIVOT (
    CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
    FOR month IN (1 JAN, 2 FEB)
)
ORDER BY destination
""")