Aquí se resolverán un par de preguntas de los ejercicios extra del capítulo 3.

# StructField

**Cuando se define un schema al definir un campo por ejemplo `StructField('Delay', FloatType(), True)` ¿qué significa el último parámetro Boolean?**

El último parámetro Boolean determina si el campo es `Nullable`, es decir, si puede contener un valor vacío o `null`.

# DataSet vs DataFrame

* Un **DataFrame** no es tipado; un **DataSet** sí.
* Un **DataSet** da errores de compilación. Un **DataFrame** solo los da en ejecución.
* Y bueno seguramente haya más cosas pero sabe usted que a mí esto no se me da bien.