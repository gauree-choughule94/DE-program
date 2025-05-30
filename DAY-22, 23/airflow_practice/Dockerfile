# # FROM apache/airflow:2.8.2-python3.8

# # # USER root

# # # needed for Spark
# # # RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

# # USER airflow

# # # RUN pip install --no-cache-dir pyspark==3.3.2 requests  mysqlclient apache-airflow-providers-mysql


FROM apache/airflow:2.8.2-python3.8

USER root

RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    && apt-get clean

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-mysql mysqlclient pyspark==3.3.2 requests pandas


# FROM apache/airflow:2.8.2-python3.8

# USER root

# RUN apt-get update && apt-get install -y \
#     libpq-dev \
#     build-essential \
#     && apt-get clean

# USER airflow

# RUN pip install --no-cache-dir \
#     apache-airflow-providers-postgres \
#     psycopg2-binary \
#     pyspark==3.3.2 \
#     requests \
#     pandas
