import scala.util.Random._
import org.apache.spark.sql.functions._

// Creamos la clase
case class Bloggers(id:Int, first:String, last:String, url:String, date:String, hits:Int, campaigns:Array[String])

// Creamos el dataset
val bloggers = "blogs.json"
val bloggersDS = spark.read.format("json").option("path",bloggers).load().as[Bloggers]

// 2. Creación de datos aleatorios

// Case Class para el DS
case class Usage(uid:Int, uname:String, usage:Int)
val r = new scala.util.random(42)

// Creamos 1000 instancias de Usage
val data = for (i <- 0 to 1000)
    yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
                r.nextInt(1000))) // Esto crea datos aleatoriamente.

val dsUsage = spark.createDataset(data)

// 3. Transformaciones de datos

// Mostramos los usuarios cuyo uso exceda 900: filter
dsUsage.filter(d => d.usage > 900)
                     .orderBy(desc("usage"))
                     .show(5, false)

// Lo mismo de antes: UDF
def filterWithUsage(u: Usage) = u.usage > 900
dsUsage.filter(filterWitHUsge(_)).orderBy(desc("usage")).show()

// Uso de una función lambda que no devuelve un booleano para hacer un mapeo
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 }).show()

// Función UDF para lo mismo
def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * 0.15 else usage * 0.50
}

dsUsage.map(u => {computeCostUsage(u.usage)}).show()

// Asociación del coste con los usuarios
case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

// Computamos el coste con Usage como parámetro y devolvemos un objeto nuevo
def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * .15 else u.usage * .50
    UsageCost(u.uid, u.uname, u.usage, v)
}

dsUsage.map(u => {computeUserCostUsage(u)}).show()