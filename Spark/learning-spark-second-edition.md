# Learning Spark, 2nd Edition

## Introducción

**Apache Spark** es un framework de computación en clúster open-source.

Debido al propósito del aprendizaje, este resúmen solo constará de los capítulos 1 al 6. Molestien las disculpas.

## Prefacio, Prólogo, Presurización, Protozoo, Praliné
### Organización del libro
* Este libro no se centra en operaciones con RDD a bajo nivel. En lugar de eso se centra en las APIs estructuradas.
* Capítulo 1: Teoría e Introducción Histórica.
* Capítulo 2: Instalación y configuración.
* Capítulos 3 a 6: Uso de los DataFrames y APIs estructuradas.
* Capítulos 7-12 (no cubiertos en este resumen): Optimización, Streaming, Data Lakes, Machine Learning, Despliegue y Spark 3.0 (epílogo) respectivamente.
### Uso de los ejemplos de código
[Hay un repositorio disponible con los ejemplos completos.](https://github.com/databricks/LearningSparkV2) - Se usa Apache Spark 3.0 y JDK 1.8.0.

## Capítulo 1
*Introducción a Apache Spark - Introduce la evolución del big data *(teoría e historia del big data y la herramienta)*
* Todo lo que es Big Data ahora mismo viene más o menos por Google: Crearon el *Google File System (GFS)*, el *MapReduce* y *Bigtable*, herramientas muy importantes.
  * *GFS*: Sistema de ficheros distribuido.
  * *BigTable*: Almacenamiento escalable para datos estructurados en *GFS*.
  * *MapReduce*: Paradigma de programación paralela funcional para procesamiento de datos a gran escala en GFS y BigTable.
* Trabajo propietario pero llevaron a ideas innovadoras en el mundo del *open-source*.
  * ***Hadoop File System (HDFS)***
* *MapReduce* tenía ciertos problemas.
  * Difícil de gestionar y administrar (complejidad operacional).
  * API requiere mucha configuración, frágil tolerancia a fallos.
  * Mucha escritura a disco que conlleva gran reducción del rendimiento en trabajos grandes.
  * Pobre compatibilidad con trabajos de machine-larning, streaming o consultas interactivas.
    * Para esto surgieron sistemas como Hive, Impala, etc. con sus propias API y configuraciones de cluster -> **Aumento de complejidad operacional** y **aumento de la curva de dificultad**.
### Spark
* ***Spark***: La idea es arreglar los defectos de MapReduce creando algo **más simple, más rápido y más fácil**.
  * Mejora de 10 a 20 veces más rápido que Hadoop MapReduce (mucho más en la actualidad).
  * Filosofía: **velocidad**, **facilidad de uso**, **modularidad**, **extensibilidad**.

#### Velocidad
* La implementación interna se beneficia de los avances recientes en el HW.
* Las computaciones de consultas se hacen mediante un DAG (grafo dirigido acíclico). Puede descomponerse en tareas ejecutadas en paralelo en nodos *worker* en el cluster.
* Tungsten (motor de ejecución física) genera código compacto para su ejecución.

#### Facilidad de Uso
* Abstracción fundamental de la estructura de datos lógicos llamada ***RDD*** a partir de la cual surgen estructuras de más alto nivel como los **DataFrames** o los **Datasets**.
* **Operaciones**: **Transformaciones** y **Acciones**. Esto lleva a un modelo simple de programación para construir aplicaciones.

#### Modularidad
* Las operaciones Spark funcionan para todo tipo de cargas y en multitud de lenguajes: Scala, Java, Python, SQL, R.
* Spark ofrece bibliotecas unificadas con APIs bien documentadas para los siguientes componentes: SparkSQL (capítulos 4 a 6), Spark Structured Streaming, Spark MLlib (Machine Learning) y GraphX. Todo bajo el mismo motor.
* Una sola aplicación puede hacerlo todo, sin necesidad de meterse en varias APIs o motores. Spark te lo unifica.

#### Extensibilidad
* Hadoop incluía almacenamiento y computación pero Spark los separa.
* Spark puede leer datos de muchas fuentes (Hadoop, Cassandra, HBase, Mongo, Hive, RDBMs...) todo procesado en memoria (Extensible a otras fuentes como Kafka, Kinesis, Azure Storage, Amazon S3...). **Abstracción lógica de datos**.
* Paquetes de terceros para el creciente ecosistema. Conectores para otras fuentes de datos.

#### Stack Unificado
* Spark ofrece componentes (SparkSQL, Spark MLlib, Spark Structured Streaming, GraphX) como bibliotecas separadas del *core*, que se encarga de convertir tu aplicación a un DAG para su ejecución en el propio *core*.

##### SparkSQL
* Trabaja bien con datos estructurados. Puedes leer datos de un RDBMS u otros formatos estructurados o semiestructurados (CSV, JSON, etc.) y construir tablas permanentes o temporales en Spark. Permite el uso de SQL. (Ver capítulos 4-6)

##### Spark MLlib
* Biblioteca con algoritmos comunes de Machine Learning. Permite la extracción y transformación de características; la construcción de *pipelines*, la persistencia de modelos, todo durante el despliegue. También incluye otras cuestiones de álgebra lineal y otras cosas para construcción de modelos.

##### Spark Structured Streaming
* Modelo de streaming continuo y API de streaming estructurado construido sobre Spark SQL y APIs de DataFrames. Es necesario para los desarrolladores de big data combinar y reaccionar a grandes cantidades de datos estáticos y *streaming* a partir de Kafka y otras fuentes. El modelo ve un stream como una tabla continuamente creciente, con nuevas filas añadidas al final. Puede tratarse como una tabla estructurada y consultarla como si fuera una tabla estática normal.
* Por debajo se gestionan todos los aspectos de tolerancia a fallos y semántica de datos tardíos. Se obvia el modelo de DStreams de Spark 1.x (capítulo 8).

##### GraphX
* Biblioteca para manipulación de grafos (redes sociales, rutas, puntos de conexión, grafos de topología de red) y realización de operaciones paralelas sobre los mismos. Ofrece algritmos de grafos estándar (análisis, conexiones, viajes, como PageRank, Connected Components y Triangle Counting).

#### Ejecución distribuida
Spark es un motor de procesamiento de datos distribuidos. Para entenderlo mejor, vamos a ver lo que hace cada componente.
##### Driver
Aplicación responsable de instanciar una `SparkSession`.
* Se comunica con el `cluster manager`, pide recursos (CPU, memoria, etc) a dicho manager para los `executors` y transforma las operaciones Spark en computaciones DAG, las programa y distribuye sus `tasks` entre los `executors`.
##### SparkSession
Conducto unificado para todas las operaciones y datos de Spark. Reune todos los contextos facilitando el trabajar con Spark.
* Creación de parámetros de ejecución JVM, definición de DataFrames y Datasets, lectura de fuentes, acceso a metadatos, ejecución de consultas SparkSQL.
* Es un punto de entrada unificado para las funcionalidades Spark.
* Aplicación standalone: puedes crear una `SparkSession` usando APIs de alto nivel del lenguaje escogido. En la shell se crea por ti, y es accesible mediante una variable global llamada `spark` o `sc`.
##### Cluster Manager
Responsable de gestionar y asignar recursos a los nodos del clúster en el que se ejecuta la aplicación.
* Soporte para Apache Hadoop YARN, Apache Mesos, Kubernetes, y *standalone*.
##### Spark Executor
Se ejecuta en cada nodo worker del cluster. Se comunican con el programa driver y son responsables de la ejecución de tasks en los workers. *Normalmente*, se ejecuta un Executor por nodo.
##### Modos de despliegue
Diferentes configuraciones y entornos. El cluster manager es agnóstico en este sentido, así que puede desplegarse donde se requiera:
| Modo | Driver | Executor | CM |
| ---- | ------ | -------- | -- |
| **Local** | Se ejecuta en un JVM (un nodo) | Mismo JVM que el driver | Misma máquina |
| **Standalone** | Cualquier nodo del cluster | Cada nodo del cluster lanza su JVM Executor | Asignado arbitrariamente a cualquier máquina del cluster |
| **YARN (cliente)** | En un cliente fuera del cluster | Contenedor NodeManager de YARN | Resource Manager de YARN trabaja con el Application Master para asignar los contenedores en NodeManagers para executors (que qué?) |
| **YARN (cluster)** | Con el YARN Application Master | Igual que arriba | Igual que arriba
| **Kubernetes** | En una vaina de Kubernetes | Cada worker en su propia vaina | Kubernetes Master |

##### Particiones y datos distribuidos
Los datos físicos se distribuyen en particiones en HDFS o la nube. Cada partición es tratada como una abstracción de datos de alto nivel, **un DataFrame en memoria**. Se hace lo posible para que cada Executor reciba una tarea que requiera la lectura de la partición más cercana en la red (localidad de datos).

El particionamiento permite un paralelismo más eficiente. Así los ejecutores procesan datos cercanos, minimizando el ancho de banda usado.

---

## Capítulo 2
*Descarga de Apache Spark y Configuración Inicial - Muestra el proceso de lo dicho*
### Proceso de Instalación.
**DISCLAIMER MUY GRANDE**: *Yo* he tenido varios problemas de configuración y compatibilidad siguiendo la guía del libro. *Yo* (mi persona personal e intransferible). Así que voy a indicar lo que *yo* he hecho para que me funcionen Spark y PySpark en mi máquina. Es *posible* que quien lea esto no haya tenido esos problemas siguiendo la misma guía u otra distinta; en cuyo caso, mi enhorabuena. En cualquier caso, **esta es la guía de instalación *problem-free* de Spark en Windows 10.**

Especificaciones de lo instalado:
* `Hadoop 3.3.1`
* `Spark 3.1.2`
* `Java 8` (última versión, en mi caso la `331` a fecha de escribir esto)
* `Python 3.10.4` (también la última versión).

#### Instalaciones previas: Java y Python

Lo primero que hay que hacer es [instalar Java](https://www.oracle.com/java/technologies/downloads/#java8-windows) e [instalar Python](https://www.python.org/downloads/).

* Especialmente en el caso de Java, hay que **prestar mucha atención al directorio de instalación**, pues será importante más adelante para configurar las variables de entorno necesarias. Mi preferencia ha sido instalarlo en `C:\Java\<java-ver>`, pero cualquier otra carpeta es igualmente válida.
* En Python es posible que al instalarlo aparezca la Tienda de Windows. Es preferible instalarla desde ahí si ocurriera para que el sistema reconozca la instalación más agilmente.

#### Descarga de archivos
Los archivos disponibles en la carpeta donde está ubicado este archivo son dos (`hadoop-3.3.1.zip` y `Spark.zip`).
1. Descargarlos.
2. Descomprimir sus contenidos en una carpeta a elegir. Nuevamente, mi preferencia ha sido descomprimirlos en sendas carpetas en `C:`, pero cualquier otra carpeta es igualmente válida **siempre que se tenga en cuenta la ruta**.
3. Verificar que se ha realizado correctamente: a veces al descomprimir archivos por descuido propio o del creador del .zip se crean carpetas secundarias, del tipo `C:\Spark\Spark\<archivos varios>`, que son innecesarias y nos dificultan el asunto; así que es interesante verificar que se han descomprimido correctamente sin carpetas adicionales.

##### winutils
Si no se descarga este archivo, es muy posible que de un error por su inexistencia. Así que accediendo a [este otro repositorio](https://github.com/kontext-tech/winutils), nos dirigimos a la versión de Hadoop que hemos descargado (en nuestro caso, `hadoop-3.3.1`) y descargamos el archivo `winutils.exe`.

Ahora colocamos el archivo descargado en la carpeta `bin` dentro de donde hayáis descomprimido Hadoop (en mi caso, por ejemplo, sería `C:\hadoop-3.3.1.\bin\winutils.exe` donde estaría localizado).

#### Configuración de variables

El último paso que vamos a realizar es configurar las variables de entorno; así que nos dirigimos a dichas variables (con pulsar la tecla de Windows y escribir "variables de entorno" suele bastar; pulsando en la ventana que aparece en el botón de la derecha abajo "Variables de entorno") y en las variables de usuario, añadimos las siguientes (pulsando en *Nueva...* en la parte superior):
* `HADOOP_HOME` - Ruta donde se ha ~~instalado~~ descomprimido Hadoop (en mi caso, `C:\hadoop-3.3.1`)
* `JAVA_HOME` - Ruta donde se ha instalado Java con la versión (en mi caso, `C:\Java\jdk1.8.0_331`)
* `SPARK_HOME` - Ruta donde se ha ~~instalado~~ descomprimido Spark (en mi caso, `C:\Spark`)

Ahora vamos a modificar el PATH para que podamos acceder al comando. En la variable `Path` añadimos lo siguiente:
* `%SPARK_HOME%\bin;%JAVA_HOME%\bin;%SPARK_HOME%\python;%HADOOP_HOME%\bin` (pueden añadirse una a una, asumo).

#### Comprobacion

Finalmente para comprobar que funciona basta con irse a la consola más cercana y ejecutar, primero, `spark-shell` (y comprobar que funciona con `sc`, `spark`, y luego cualquier comando que uno quiera) y después salir y ejecutar `pyspark`.

Asumiendo que no me falte nada, con esto ya estarían funcionando Spark y PySpark en local en Windows 10 en consola. (El acceso mediante `localhost:4040` también debería funcionar)

En cualquier caso, si hubiera algún fallo, no duden en contactar conmigo como buenamente se pueda para ver si es que me ha faltado algo que es muy probable.

---

### Uso de la Shell

Hay una shell para cada servicio: `spark-shell`, `pyspark`, `spark-sql` y `sparkR`. Todas tienen soporte para conexiones al cluster y carga distribuida de los datos a los workers.

Dado que nosotros hemos instalado Spark en nuestra máquina local, Spark se ejecutará en modo local (ver la tabla anterior: todo se ejecuta en el mismo JVM).

### Conceptos

* **Application**: Programa construido en Spark mediante APIs. Consiste de un driver y executors. Puede estar formada de uno o más **Jobs**.
* **SparkSession**: Objeto creado por el driver (automáticamente en una shell interactiva, creada por el usuario en una aplicación). Es lo que permite usar las APIs.
* **Job**: Computación paralela compuesta de varias **Tasks**. Se crea en respuesta a una **acción** Spark. Cada Job se transforma en un **DAG**, siendo esto el plan de ejecución. Cada nodo en un DAG puede ser uno o más **Stages**.
* **Stage**: Conjuntos menores de tareas dentro de un Job, que dependen entre sí. Se crean según si las operaciones pueden hacerse en serie o en paralelo. Compuesta de **Tasks**.
* **Task**: Federadas por cada ejecutor, cada task mapea a un único núcleo y trabaja en una única partición.

#### Transformaciones y Acciones
* **Transformaciones**: Transforman un DataFrame en otro sin alterar los datos originales (inmutabilidad). Devuelven los resultados transformados de la operación realizada en un nuevo DataFrame. Se evalúan *lazily*, esto quiere decir que se van guardando en un *lineage* y más tarde en la ejecución, Spark puede reordenar estas transformaciones, juntarlas, u **optimizarlas** en general.
* **Acciones**: Enciende la evaluación *lazy*, así que todas las transformaciones se ejecutan, y después se ejecuta la acción.

Cada cosa tiene su utilidad: *lineage* e *inmutabilidad* dan tolerancia a fallos, mientras que la cadena de transformaciones da una mejor optimización.

#### Transformaciones Narrow y Wide
Las transformaciones pueden clasificarse según tengan *dependencias narrow* o *dependencias wide*.

Si una partición de salida se computa a partir de una sola partición de entrada, es **narrow**.

Si una partición de salida requiere la lectura de más de una partición, es **wide**.

### SparkUI
Spark incluye una interfaz web en el puerto 4040 (por defecto) donde ver métricas y estadísticas de las tasks ejecutándose, el uso de memoria, información sobre el entorno, los executors, las consultas, etc. En modo local se accede mediante `http://localhost:4040`. También tiene herramientas para la visualización del DAG.

### Ejemplo práctico: Conteo de M&Ms

Vamos a observar un ejemplo práctico de uso de lo aprendido hasta ahora.

Se nos da un archivo de gran calibre y la estructura de dicho archivo. Tenemos que computar agregar por un valor numérico y agrupar por el resto de valores no numéricos.

El programa completo está disponible en el libro. Aquí vamos a intentar mostrar un par de instrucciones de interés.

1. Lo primero, los imports:
```
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
```
Cargan las partes importantes que vamos a necesitar para este script.

2. Antes de empezar, una comprobación:
```
if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: mnmcount <file>", file=sys.stderr)
    sys.exit(-1)
```
Si el método que estamos ejecutando es el principal, todo lo que pongamos a continuación se ejecutará.

Dentro de eso, tenemos que comprobar que el programa conste de dos argumentos (el nombre del programa y el archivo a leer), porque si no no funcionará. Para ello, si no consta de dos argumentos, imprimimos por la salida estándar de errores cómo se usa, y salimos del programa.

3. Ahora creamos la `SparkSession`.
```
  spark = (SparkSession
    .builder
    .appName("PythonMnmCount")
    .getOrCreate())
```
Como se ha dicho anteriormente, si estamos creando una aplicación (como es el caso) no se nos dará una `SparkSession` por defecto, sino que tendremos que crearla nosotros mismos, que es lo que estamos haciendo en el código anterior, dotándole del nombre de aplicación "PythonMnmCount", y asignándolo a la variable de nombre spark (por costumbre).

4. Leemos el nombre del archivo y luego leemos el archivo en sí mismo en formato csv.
```
  mnm_file = sys.argv[1] # Nos da el nombre de archivo.
  mnm_df = (spark.read.format("csv") # El formato del archivo es csv.
    .option("header","true") # Este CSV tiene header.
    .option("inferSchema","true") # Que deduzca la estructura.
    .load(mnm_file)) # Cargamos el archivo.
```
*Nota: Los tres campos son "State" (Estado, cadena), "Color" (ídem, cadena) y "Count" (conteo, int)*

5. Ahora que tenemos el archivo cargado, ejecutamos las operaciones necesarias para agrupar el *Count* por *State* y por *Color*:
```
  count_mnm_df = (mnm_df
    .select("State","Color","Count")
    .groupBy("State","Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))
```

6. Por último, ejecutamos una acción que cargará todas esas transformaciones:
```
  count_mnm_df.show(n=60, truncate=False)
  print("Total Rows = %d" % (count_mnm_df.count()))
```

7. Terminamos la ejecución con `spark.stop()`.

#### Ejecución del ejemplo
1. Guardamos el script python generado
2. Descargamos el archivo .csv [disponible en el repositorio de github](https://github.com/databricks/LearningSparkV2/blob/master/chapter2/py/src/data/mnm_dataset.csv) y lo guardamos en la misma carpeta que el script python. [^1]
3. Nos dirigimos a la consola más cercana y situándonos en la carpeta donde hayamos guardado ambos archivos, ejecutamos el siguiente comando:
```
> spark-submit <nombrArchivoPython>.py mnm_dataset.csv
```
El resultado será algo como esto:
```
+-----+------+-----+
|State|Color |Total|
+-----+------+-----+
|CA   |Yellow|1807 |
|WA   |Green |1779 |
|OR   |Orange|1743 |
|TX   |Green |1737 |
|TX   |Red   |1725 |
[...]
|WY   |Orange|1595 |
|UT   |Green |1591 |
|WY   |Brown |1532 |
+-----+------+-----+

Total Rows = 60
```

[^1]: Se hará así por comodidad, realmente no es necesario mientras se sepa la ruta.

#### Otro ejemplo que es exactamente el mismo
Como vemos, esto agrupa por estado y por color. Veamos cómo se filtra para ver los colores de solo un estado:

Para eso tenemos que agregar la siguiente línea.
```
  ca_count_mnm_df = (mnm_df
    .select("State","Color","Count")
    .where(mnm_df.State == "CA")) <-- ESTA
    .groupBy("State","Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total",ascending=False))
```

#### Breve comentario: Log4j
Es posible que a la hora de ejecutar el script salgan un montón de líneas que comiencen por INFO. Esto se debe a la configuración de *logging* y se puede eliminar de la siguiente manera:

1. Copiar el archivo `log4j.properties.template` a `log4j.properties`.
2. En éste, cambiar `log4j.rootCategory=INFO` por `log4j.rootCategory=WARN`.

De esta manera solo se mostrarán líneas de advertencia y error.

#### Código en Scala
Exactamente igual con cambios de sintaxis. Debería quedar algo así:
```
package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
  def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("MnMCount")
        .getOrCreate()
      
      if (args.length < 1) {
        print("Usage: MnMCount <mnm_file_dataset>")
        sys.exit(1)
      }

      val mnmFile = args(0)

      val mnmDF = spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(mnmFile)

      val countMnMDF = mnmDF
        .select("State","Color","Count")
        .groupBy("State","Color")
        .agg(Count("Count").alias("Total"))
        .orderBy(desc("Total"))

      countMnMDF.show(60)
      println("Total Rows = ${countMnMDF.count()})
      println()

      val caCountMnMDF = mnmDF
        .select("State","Color","Count")
        .where(col("State") === "CA")
        .groupBy("State","Color")
        .agg(Count("Count").alias("Total"))
        .orderBy(desc("Total"))

      caCountMnMDF.show(10)

      spark.stop()
  }
}
```

**Sin embargo**, la diferencia entre ejecutar en Scala y en Python es muy grande y es la siguiente:
* Python es un lenguaje interpretado: no requiere compilación (puede hacerse pero no es necesario).
* Sin embargo, Scala sí requiere compilación. Nos centraermos en eso en el siguiente apartado.

### Compilación Scala
Necesitaremos usar el Scala Build Tool (sbt), descargable [aquí](https://www.scala-sbt.org)

Hay una serie de elementos a tener en cuenta para compilar un archivo scala.

1. Tenemos que crear un archivo `build.sbt` en la carpeta de creación de la aplicación, con el siguiente contenido:
```
name := "main/scala/chapter2" // El paquete del archivo scala que vamos a compilar.
version := "1.0" // Versión de dicho paquete.
scalaVersion := "2.12.10" // Versión de scala.
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.1.2",
    "org.apache.spark" %% "spark-sql" % "3.1.2"
)
// Las dependencias necesarias, con el paquete en el que se hallan, el paquete concreto que importamos, y la versión de spark (o del paquete, más bien) que importamos.
)
```

2. Una vez hecho esto, podemos ejecutar el siguiente comando.
```
> sbt clean package
```
Este compila el paquete señalado. Te señalará la existencia de errores en el proceso y finalmente compilará si no los hubiera.

3. Por último, ejecutamos el archivo compilado así:
```
> spark-submit --class main.scala.chapter2.MnMcount target/scala-2.12/main-scala-chapter2_2.12-1.0.jar mnm_dataset.csv
```
El archivo `jar` es generado por la compilación, y puede encontrarse en `./target/scala-2.12` (o la versión que toque), junto a otras carpetas.

Los argumentos son: la clase, que se saca a partir del archivo .scala (con el paquete y el objeto que se crea); la ubicación del archivo .jar generado relativo a desde donde se ejecuta `spark-submit`; y los argumentos que requiera la aplicación (en nuestro caso el archivo csv).

*Mucho cuidado con las rutas*. La mayoría de problemas de ejecución o compilación en este contexto vienen dados por unas rutas incorrectamente configuradas.

En resumen, la diferencia principal es que **Scala debe ser complicado**.

---

## Capítulo 3
*APIs Estructuradas*

### RDDs
RDD es la abstracción más básica de Spark, y la de más bajo nivel. Tienen **dependencias**, **particiones** y **funciones compute** que se computan. A partir de aquí se construyen funcionalidades de más alto nivel.
* Las **dependencias** indican a Spark como se construye un RDD concreto, y cómo replicarlo a partir de esas dependencias.
* Las **particiones** le dan a Spark la habilidad de paralelizar el trabajo en varios executors.
* Las **funciones compute**  producen un Iterador `Iterator[T]` de tipo `T` para los datos en el RDD.

Problemas: la función es opaca a Spark, así que Spark no sabe qué hay ahí, por lo que no puede optimizar esa operación. El tipo `T` del Iterador también es abstracto, así que no sabe si estás accediendo una columna de cierto tipo. Así que todo lo que puede hacer Spark es serializar el objeto sin usar compresión. Esto fastidia la habilidad de Spark de optimizar la computación de forma eficiente. La solución: **darle estructura**.

### Estructura: Principios
* Mediante el uso de patrones comunes de análisis de datos expresados en operaciones de alto nivel consigues claridad.
* Usando operadores comunes de un DSL disponibles mediante las respectivas APIs consigues decirle a Spark qué quieres hacer con los datos para que Spark construya un plan de consultas eficiente.
* Con orden y estructura en tus datos, como en una tabla SQL o una hoja de cálculo, y con tipos de datos soportados, puedes organizar mejor tus datos.

Tener estructura es muy útil para la eficiencia en tiempo y espacio en los componentes Spark. Las ventajas se basan en cuatro principios: **Expresividad**, **Sencillez**, **Componibilidad** y **Uniformidad**.

* **Expresividad**: Las operaciones de alto nivel le indican a Spark qué hacer en vez de cómo hacerlo. *Podemos expresar más con menos*.
* **Sencillez**: Al usar operaciones de alto nivel, *el código resulta más fácil de leer*.
* **Componibilidad**: Con operaciones de alto nivel, *los componentes trabajan mejor entre sí* al encargarse Spark de optimizar la ejecución.
* **Uniformidad**: *El código en los diversos lenguajes es más uniforme* al usar operaciones de alto nivel.

### DataFrames
Se comportan como tablas distribuidas almacenadas en memoria, con columnas y *schemas* nombrados. Cada columna tiene un tipo específico. Es como una tabla. Cuando se visualizan los datos son inteligibles y fáciles de manejar.

#### Tipos básicos
Spark soporta tipos básicos internos. Todos heredan de `DataTypes` (excepto `DecimalType`).

| DataType      | Scala                  | Python            | API                     |
| --------------| ---------------------- | ----------------- | ----------------------- |
| `ByteType`    | `Byte`                 | `int`             | `DataTypes.ByteType`    |
| `ShortType`   | `Short`                | `int`             | `DataTypes.ShortType`   |
| `IntegerType` | `Int`                  | `int`             | `DataTypes.IntegerType` |
| `LongType`    | `Long`                 | `int`             | `DataTypes.LongType`    |
| `FloatType`   | `Float`                | `float`           | `DataTypes.FloatType`   |
| `DoubleType`  | `Double`               | `float`           | `DataTypes.DoubleType`  |
| `StringType`  | `String`               | `str`             | `DataTypes.StringType`  |
| `BooleanType` | `Boolean`              | `bool`            | `DataTypes.BooleanType` |
| `DecimalType` | `java.math.BigDecimal` | `decimal.Decimal` | `DecimalType`           |

#### Tipos complejos
Igualmente, Spark soporta la declaración de tipos complejos. A veces los datos llegarán en maneras más complejas.

| DataType          | Scala                         | Python                        | API                                          |
| ----------------- | ----------------------------- | ----------------------------- | -------------------------------------------- |
| `BinaryType`      | `Array[Byte]`                 | `bytearray`                   | `DataTypes.BinaryType`                       |
| `TimestampType`   | `java.sql.Timestamp`          | `datetime.datetime`           | `DataTypes.TimestampType`                    |
| `DateType`        | `java.sql.Date`               | `datetime.date`               | `DataTypes.DateType`                         |
| `ArrayType`       | `scala.collection.Seq`        | Lista, tupla o array          | `DataTypes.createArrayTupe(ElementType)`     |
| `MapType`         | `scala.collection.Map`        | `dict`                        | `DataTypes.createMapType(keyType,valueType)` |
| `StructType`      | `org.apache.spark.sql.Row`    | Lista o tupla                 | `StructType(ArrayType[fieldTypes])`          |
| `StructField`     | Valor correspondiente al tipo | Valor correspondiente al tipo | `StructField(name, dataType, [nullable])`    |

También es importante más que los tipos, ver cómo encajan en un *schema*.

#### Schemas
Un ***schema*** define los nombres de las columnas y los tipos de datos asociados para un DF (importantes al leer de fuente de datos externa). Los beneficios de montar un *schema* a priori en vez de a posteriori son:
* Spark no tiene que inferir tipos de datos.
* Spark no tiene que crear un *Job* separado para leer una porción grande de tus datos para inferir el *schema* (mucho consumo para un archivo grande)
* Detección de errores si los datos no coinciden con el *schema*.

Los schemas se definen de la siguiente manera:
* O bien programáticamente, (mediante funciones que crean `StructField`):
  * Scala: `val schema = StructType(Array(StructField("<nombre>",<Type>,<nullable?>[,StructField("<nombre>",<Type>,<nullable?>),...]))`.
  * Python: `schema = StructType([StructField("<nombre>",<Type>,<nullable?>[,StructField("<nombre>",<Type>,<nullable?>),...]])`.
* O bien mediante una cadena DDL (más simple y fácil de leer):
  * Scala: `val schema = "author STRING, title STRING, pages INT"`.
  * Python: `schema = "author STRING, title STRING, pages INT"`.

A veces se usarán ambos. Si usamos `<DF>.schema` se nos devolverá la estructura en lista programática.

Esto también ocurre si se lee a partir de un fichero externo, como un JSON.

#### Operando sobre schemas: columnas y expresiones
* `Column` es un objeto con métodos públicos. Puede hacerse mucho con ello, como expresiones (`expr(columnName * 5)`).
* Por otro lado, `col()` devuelve una columna concreta.

Ejemplo:
```
> val x = (expr("columnName - 5") > col(anotherCol))
```
`expr` es parte de `pyspark.sql.functions` (Python) / `org.apache.spark.sql.functions` (Scala).

Sobre todo sirven para realizar columnas condicionales, campos calculados, etc.

[quillo de aquí palante mañana lo tienes que repasar eh](#schemas)

y luego están las rows que también son muy interesantes


---
## Capítulo 4
*Spark SQL y DataFrames: Fuentes Internas de Datos*

---
## Capítulo 5
*SparkSQL y DataFrames: Interacción con Fuentes Externas de Datos*

---
## Capítulo 6
*SparkSQL y Datasets*
