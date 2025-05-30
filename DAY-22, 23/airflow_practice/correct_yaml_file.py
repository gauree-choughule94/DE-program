# version: '3.3'
# services:
#   mysql:
#     image: mysql:8.0
#     # environment:
#     #   MYSQL_ROOT_PASSWORD: root
#     #   MYSQL_DATABASE: airflow
#     #   MYSQL_USER: airflow
#     #   MYSQL_PASSWORD: airflow
#     environment:
#       - MYSQL_ROOT_PASSWORD=airflow_pass
#       # - MYSQL_USER=root
#       - MYSQL_PASSWORD=airflow_pass   # add only for non root user
#       - MYSQL_DATABASE=airflow
#     ports:
#       - "3308:3306"
#     volumes:
#       - mysql_data:/var/lib/mysql
 
#   spark:
#     image: bitnami/spark:latest
#     container_name: spark_1
#     depends_on:
#       - mysql
#     environment:
#       - SPARK_MODE=master
#     volumes:
#       - ./spark_app:/app
#     command: bash -c "sleep infinity"
 
#   airflow-webserver:
#     build: .
#     restart: always
#     environment:
#       AIRFLOW__CORE__EXECUTOR: LocalExecutor
#       AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: mysql+mysqldb://root:airflow_pass@mysql/airflow
#       AIRFLOW__CORE__FERNET_KEY: ''
#       AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
#       AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
#       AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'true'
#     volumes:
#       - ./dags:/opt/airflow/dags
#     ports:
#       - "8090:8080"
#     depends_on:
#       - airflow-init
#       - airflow-scheduler
#     command: webserver
 
#   airflow-scheduler:
#     build: .
#     restart: always
#     environment:
#       AIRFLOW__CORE__EXECUTOR: LocalExecutor
#       AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: mysql+mysqldb://root:airflow_pass@mysql/airflow
#     volumes:
#       - ./dags:/opt/airflow/dags
#     depends_on:
#       - airflow-init
#       - mysql
#     command: scheduler
 
#   airflow-init:
#     build: .
#     restart: "no"
#     environment:
#       AIRFLOW__CORE__EXECUTOR: LocalExecutor
#       AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: mysql+mysqldb://root:airflow_pass@mysql/airflow
#     volumes:
#       - ./dags:/opt/airflow/dags
#     command: >
#       bash -c "
#         sleep 10 &&
#         airflow db init &&
#         airflow users create \
#           --username admin \
#           --firstname Admin \
#           --lastname User \
#           --role Admin \
#           --email admin@example.com \
#           --password admin
#       "
 
# volumes:
#   mysql_data: