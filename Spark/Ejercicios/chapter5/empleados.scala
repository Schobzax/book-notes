// Cargar con spark datos de empleados y departamentos
// Mediante joins mostrar toda la información de los empleados además de su título y salario
// Diferencia entre Rank y dense_rank

// Carga
val empleadosDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/employees").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "employees").option("user","root").option("password", "rootpassword").load()
val deptosDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/employees").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "departments").option("user","root").option("password", "rootpassword").load()
val empdeptDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/employees").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "dept_emp").optoin("user", "root").option("password", "rootpassword").load()
val salariosDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/employees").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "salaries").optoin("user", "root").option("password", "rootpassword").load()
val titlesDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/employees").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "titles").optoin("user", "root").option("password", "rootpassword").load()

// Muestra de información
// Debido al tamaño del dataset, se va a dividir en tres consultas.

// 1. Consultamos el departamento de los empleados
val empleados_departamentoDF = empleadosDF.join(
    empdeptDF, ("emp_no")
).join(
    deptosDF, ("dept_no")
).select("emp_no","first_name","last_name","dept_no","dept_name","from_date","to_date").orderBy("emp_no","from_date")

empleados_departamentoDF.show()

// 2. Consultamos el salario de los empleados
val empleados_salarioDF = empleadosDF.join(
    salariosDF, ("emp_no")
).select("emp_no","first_name","last_name","salary","from_date","to_date").orderBy("emp_no","from_date")

empleados_salarioDF.show()

// 3. Consultamos el titulo de los empleados
val empleados_tituloDF = empleadosDF.join(
    titlesDF, ("emp_no")
).select("emp_no","first_name","last_name","title","from_date","to_date").orderBy("emp_no","from_date")

empleados_tituloDF.show()

// Diferencia entre Rank y Dense_Rank

// Rank te da el ranking dentro de la partición ordenada, asignándose por posición. Si tienes tres elementos en el rango 2, el siguiente sería el rango 5.
// Dense_Rank nuevamente te da el ranking de tu partición ordenada pero los rangos son consecutivos y sin saltarse ninguno.