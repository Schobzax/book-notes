// El paquete del archivo scala que vamos a compilar.
name := "main/scala/chapter2/extra/quijote" // El paquete del ejercicio quijote
// name := "main/scala/chapter2/extra/mnm" // El paquete del ejercicio M&M
version := "1.0" // Versión de dicho paquete.
scalaVersion := "2.12.10" // Versión de scala.
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.1.2",
    "org.apache.spark" %% "spark-sql" % "3.1.2"
) // Las dependencias necesarias, con el paquete en el que se hallan, el paquete concreto que importamos, y la versión de spark (o del paquete, más bien) que importamos.
