# Preguntas de reflexión

**1. Eficiencia.** Cada split() extra repite el mismo trabajo de procesar la cadena por cada línea. Con 1 millón de líneas, hacerlo 3 veces triplica ese costo de CPU. Hacerlo una sola vez en una función (parsear_linea) evita el desperdicio.

**2. Particionamiento.** Mi RDD de stocks.txt tiene 2 particiones (ver RESULTADOS/salida_proyecto_stocks.txt). Cada partición se procesa en una tarea en paralelo. Con una sola partición solo se usaría un core y se perdería el paralelismo.

**3. Lazy evaluation.** Las transformaciones (map, filter, reduceByKey) solo construyen el plan. Se ejecutan cuando llamo una acción (count, take, collect, saveAsTextFile). Esto permite que Spark optimice el plan y evite trabajo innecesario.

**4. Memoria.** collect() en un RDD de 10 GB trae todo al driver y causa OutOfMemoryError. Se evita con take(), agregaciones previas (reduceByKey) o guardando con saveAsTextFile.

**5. Optimización.** Con 100 veces más datos: más particiones, cache() de los RDD que se reutilizan (como el RDD parseado), usar DataFrames y ajustar spark.sql.shuffle.partitions.

**6. HDFS vs local.** HDFS distribuye y replica los datos entre nodos, tolera fallos de un nodo y permite que Spark procese cerca de donde están los datos. Un archivo local depende de una sola máquina.
