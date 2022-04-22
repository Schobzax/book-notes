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

---

## Capítulo 3
*APIs Estructuradas*

## Capítulo 4
*Spark SQL y DataFrames: Fuentes Internas de Datos*

## Capítulo 5
*SparkSQL y DataFrames: Interacción con Fuentes Externas de Datos*

## Capítulo 6
*SparkSQL y Datasets*