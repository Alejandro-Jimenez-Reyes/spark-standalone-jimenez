# Práctica Spark Standalone
Ejercicios 1-7 y proyecto de análisis de stocks con PySpark (Docker, apache/spark).

## Ejecución
docker run --rm -p 4040:4040 -v $(pwd):/opt/spark/work-dir apache/spark /opt/spark/bin/spark-submit /opt/spark/work-dir/ejercicios.py
