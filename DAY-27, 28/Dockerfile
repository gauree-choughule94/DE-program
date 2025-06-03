FROM apache/airflow:2.7.2-python3.8

USER root

RUN apt-get update && \
    apt-get install -y default-jdk procps iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3

USER airflow

# Set working directory to /opt/airflow (default for Apache Airflow image)
WORKDIR /opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
