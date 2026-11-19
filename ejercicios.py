import os
from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("MiPrimerAnalisis")
         .config("spark.executor.memory", "1g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
sc = spark.sparkContext
print("== Ej1 ==", sc.appName, spark.conf.get("spark.executor.memory"),
      spark.conf.get("spark.sql.shuffle.partitions"), spark.version)

rdd = sc.parallelize(list(range(1, 21)))
print("== Ej2 ==", rdd.count(), rdd.take(5), rdd.sum(), rdd.max(), rdd.min())

n = sc.parallelize(range(1, 51)).map(lambda x: x * 3)
mayores = n.filter(lambda x: x > 50)
print("== Ej3 ==", mayores.count(), mayores.take(10))

ventas = sc.parallelize([("Laptop", 1200), ("Mouse", 25), ("Laptop", 1100),
                         ("Teclado", 75), ("Mouse", 30), ("Laptop", 1300)])
totales = ventas.reduceByKey(lambda a, b: a + b)
cuentas = ventas.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
print("== Ej4 ==", totales.collect(), cuentas.collect(),
      totales.max(key=lambda x: x[1]))

ruta = "file://" + os.path.abspath("./DATOS/persons.csv")
lineas = sc.textFile(ruta)
header = lineas.first()
datos = lineas.filter(lambda l: l != header)

def nombre_edad(l):
    c = l.split(",")
    return (c[0], int(c[1]))

mayores30 = datos.map(nombre_edad).filter(lambda x: x[1] > 30)
print("== Ej5 ==", mayores30.collect(), "Total:", mayores30.count())

def parsear(l):
    c = l.split(",")
    return (c[0], c[1], c[2])

print("== Ej6 ==", datos.map(parsear).take(3))

rdd7 = sc.textFile("file://" + os.path.abspath("./DATOS/persons.csv"))
print("== Ej7 ==")
for linea in rdd7.take(5):
    print(linea)

spark.stop()
