import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("AnalisisStocks")
         .config("spark.executor.memory", "512m").getOrCreate())
sc = spark.sparkContext

path = "file://" + os.path.abspath("./DATOS/stocks.txt")
rdd = sc.textFile(path)
print("== Paso 1 ==", rdd.take(5))
print("Particiones:", rdd.getNumPartitions())

def parsear_linea(linea):
    try:
        p = linea.split(",")
        return (int(p[0]), p[1], p[2], int(p[3]), float(p[4]))
    except Exception:
        return None

datos = rdd.map(parsear_linea).filter(lambda x: x is not None).cache()
print("== Paso 2 == Líneas válidas:", datos.count())

cat_precio = datos.map(lambda x: (x[2], x[4]))
p_max = cat_precio.reduceByKey(max)
p_min = cat_precio.reduceByKey(min)
p_prom = (cat_precio.aggregateByKey((0.0, 0),
              lambda acc, v: (acc[0] + v, acc[1] + 1),
              lambda a, b: (a[0] + b[0], a[1] + b[1]))
          .mapValues(lambda x: x[0] / x[1]))
print("== Paso 3 ==")
for c, p in sorted(p_max.collect()):
    print(f"Max {c}: ${p:.2f}")
for c, p in sorted(p_min.collect()):
    print(f"Min {c}: ${p:.2f}")
for c, p in sorted(p_prom.collect()):
    print(f"Prom {c}: ${p:.2f}")

valor = datos.map(lambda x: (x[1], x[3] * x[4])).reduceByKey(lambda a, b: a + b)
top = valor.sortBy(lambda x: x[1], ascending=False)
print("== Paso 4 == Top 10 por valor de inventario:")
for nombre, v in top.take(10):
    print(f"{nombre}: ${v:,.2f}")

stats = cat_precio.aggregateByKey((float('-inf'), float('inf')),
              lambda acc, p: (max(acc[0], p), min(acc[1], p)),
              lambda a, b: (max(a[0], b[0]), min(a[1], b[1])))
vol = stats.mapValues(lambda x: (x[0] - x[1]) / x[1] * 100)
print("== Paso 5 ==")
print("Más volátiles:")
for c, v in vol.sortBy(lambda x: x[1], ascending=False).take(5):
    print(f"{c}: {v:.2f}%")
print("Más estables:")
for c, v in vol.sortBy(lambda x: x[1]).take(5):
    print(f"{c}: {v:.2f}%")

valor.saveAsTextFile("file://" + os.path.abspath("./RESULTADOS/valor_por_producto"))
stats.saveAsTextFile("file://" + os.path.abspath("./RESULTADOS/stats_por_categoria"))

mayor_prom = p_prom.sortBy(lambda x: x[1], ascending=False).first()
mas_volatil = vol.sortBy(lambda x: x[1], ascending=False).first()
print("=" * 60)
print("RESUMEN EJECUTIVO - ANÁLISIS DE STOCKS")
print("=" * 60)
print("Total de registros procesados:", datos.count())
print("Categorías únicas:", datos.map(lambda x: x[2]).distinct().count())
print(f"Categoría con mayor precio promedio: {mayor_prom[0]} (${mayor_prom[1]:.2f})")
print(f"Producto con mayor valor de inventario: {top.first()[0]} (${top.first()[1]:,.2f})")
print(f"Categoría más volátil: {mas_volatil[0]} ({mas_volatil[1]:.2f}%)")

spark.stop()
print("Análisis completado. Sesión cerrada.")
