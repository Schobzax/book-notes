# GlobalTempView vs. TempView

La diferencia es *tempView* y *globalTempView* es sutil, pero importante.

* Una **TempView** se crea dentro de una `SparkSession` dentro de una aplicación.
* Una **GlobalTempView** es visible por varias `SparkSession`s en una aplicación.

Ambas, por el hecho de ser temporales, desaparecen al cerrar la aplicación; pero al cerrar la sesión solo se cierran las **TempView** correspondientes a la misma; una **GlobalTempView** no desaparece si se cierra una o más `SparkSession`, únicamente cuando se cierra la aplicación.