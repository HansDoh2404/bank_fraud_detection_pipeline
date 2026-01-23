FROM apache/airflow:3.1.0

RUN pip install --no-cache-dir pandas pyspark findspark apache-airflow-providers-apache-spark apache-airflow[statsd]
