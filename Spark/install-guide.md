# Proceso de Instalación.
**DISCLAIMER MUY GRANDE**: *Yo* he tenido varios problemas de configuración y compatibilidad siguiendo la guía del libro. *Yo* (mi persona personal e intransferible). Así que voy a indicar lo que *yo* he hecho para que me funcionen Spark y PySpark en mi máquina. Es *posible* que quien lea esto no haya tenido esos problemas siguiendo la misma guía u otra distinta; en cuyo caso, mi enhorabuena. En cualquier caso, **esta es la guía de instalación *problem-free* de Spark en Windows 10.**

Especificaciones de lo instalado:
* `Hadoop 3.3.1`
* `Spark 3.1.2`
* `Java 8` (última versión, en mi caso la `331` a fecha de escribir esto)
* `Python 3.10.4` (también la última versión).

## Instalaciones previas: Java y Python

Lo primero que hay que hacer es [instalar Java](https://www.oracle.com/java/technologies/downloads/#java8-windows) e [instalar Python](https://www.python.org/downloads/).

* Especialmente en el caso de Java, hay que **prestar mucha atención al directorio de instalación**, pues será importante más adelante para configurar las variables de entorno necesarias. Mi preferencia ha sido instalarlo en `C:\Java\<java-ver>`, pero cualquier otra carpeta es igualmente válida.
* En Python es posible que al instalarlo aparezca la Tienda de Windows. Es preferible instalarla desde ahí si ocurriera para que el sistema reconozca la instalación más agilmente.

## Descarga de archivos
Los archivos disponibles en la carpeta donde está ubicado este archivo son dos (`hadoop-3.3.1.zip` y `Spark.zip`).
1. Descargarlos.
2. Descomprimir sus contenidos en una carpeta a elegir. Nuevamente, mi preferencia ha sido descomprimirlos en sendas carpetas en `C:`, pero cualquier otra carpeta es igualmente válida **siempre que se tenga en cuenta la ruta**.
3. Verificar que se ha realizado correctamente: a veces al descomprimir archivos por descuido propio o del creador del .zip se crean carpetas secundarias, del tipo `C:\Spark\Spark\<archivos varios>`, que son innecesarias y nos dificultan el asunto; así que es interesante verificar que se han descomprimido correctamente sin carpetas adicionales.

### winutils
Si no se descarga este archivo, es muy posible que de un error por su inexistencia. Así que accediendo a [este otro repositorio](https://github.com/kontext-tech/winutils), nos dirigimos a la versión de Hadoop que hemos descargado (en nuestro caso, `hadoop-3.3.1`) y descargamos el archivo `winutils.exe`.

Ahora colocamos el archivo descargado en la carpeta `bin` dentro de donde hayáis descomprimido Hadoop (en mi caso, por ejemplo, sería `C:\hadoop-3.3.1.\bin\winutils.exe` donde estaría localizado).

## Configuración de variables

El último paso que vamos a realizar es configurar las variables de entorno; así que nos dirigimos a dichas variables (con pulsar la tecla de Windows y escribir "variables de entorno" suele bastar; pulsando en la ventana que aparece en el botón de la derecha abajo "Variables de entorno") y en las variables de usuario, añadimos las siguientes (pulsando en *Nueva...* en la parte superior):
* `HADOOP_HOME` - Ruta donde se ha ~~instalado~~ descomprimido Hadoop (en mi caso, `C:\hadoop-3.3.1`)
* `JAVA_HOME` - Ruta donde se ha instalado Java con la versión (en mi caso, `C:\Java\jdk1.8.0_331`)
* `SPARK_HOME` - Ruta donde se ha ~~instalado~~ descomprimido Spark (en mi caso, `C:\Spark`)

Ahora vamos a modificar el PATH para que podamos acceder al comando. En la variable `Path` añadimos lo siguiente:
* `%SPARK_HOME%\bin;%JAVA_HOME%\bin;%SPARK_HOME%\python;%HADOOP_HOME%\bin` (pueden añadirse una a una, asumo).

## Comprobacion

Finalmente para comprobar que funciona basta con irse a la consola más cercana y ejecutar, primero, `spark-shell` (y comprobar que funciona con `sc`, `spark`, y luego cualquier comando que uno quiera) y después salir y ejecutar `pyspark`.

Asumiendo que no me falte nada, con esto ya estarían funcionando Spark y PySpark en local en Windows 10 en consola. (El acceso mediante `localhost:4040` también debería funcionar)

En cualquier caso, si hubiera algún fallo, no duden en contactar conmigo como buenamente se pueda para ver si es que me ha faltado algo que es muy probable.