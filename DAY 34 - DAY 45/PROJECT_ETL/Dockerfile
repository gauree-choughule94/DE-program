FROM apache/airflow:2.8.0
 
# Start as root to install system packages
USER root
 
# Install Java (for PySpark)
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean
 
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
 
# Switch to airflow user BEFORE installing Python packages
USER airflow
 
# Install Python deps as airflow user
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt