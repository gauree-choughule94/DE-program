in venv => pip install "apache-airflow==2.8.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.8.txt"

export AIRFLOW_HOME=.
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=sqlite:///${AIRFLOW_HOME}/airflow.db
airflow db init

airflow webserver -p 8088 => x

airflow users create --help

airflow users create --username admin --firstname gauree --lastname choughule --role Admin --email admin@domain.com

enter password => p@ssw0rd

airflow webserver -p 8081 [admin, p@ssw0rd ]
## docker
doc link => https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

copy from link

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.0/docker-compose.yaml'

modify below
AIRFLOW__CORE__EXECUTOR: LocalExecutor

remove below
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0

        &airflow-common-depends-on
    redis:
      condition: service_healthy


  redis:
    # Redis is limited to 7.2-bookworm due to licencing change
    # https://redis.io/blog/redis-adopts-dual-source-available-licensing/
    image: redis:7.2-bookworm
    expose:
      - 6379
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 30s
      retries: 50
      start_period: 30s
    restart: always

celery worker and flower


below in terminal
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env     => only for linux

[docker compose up airflow-init or docker-compose up -d airflow-init
]  =>  for initializing the Airflow environment [Creates necessary folders in /opt/airflow/:

    logs, dags, plugins, config]

docker compose up -d

## unpause DAG
docker compose exec webserver airflow dags unpause our_first_dag_v5

## replace user with new
docker compose run --rm airflow-init bash -c "airflow db reset -y && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"


docker exec -it 355a2a8c8609 bash => airflow dags backfill -s olddate -e startdate dagid
[airflow dags backfill -s 2021-11-01 -e 2021-11-08 dag_with_catchup_backfill_v02] => see logs

## crontab.guru[cron expression]

## install python package in airflow dockerfile container [for 2nd package only repeat 3, 5, 6]
1. add package in requirements.txt
2. add dockerfile code
3. create image dockerfile at current dir and give name as 'extending_airflow' and version latest
docker build . --tag extending_airflow:latest
4. replace/add image name in yaml file
5. add .py file in dags
6. run below in terminal
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler

## [ERROR] Connection in use: ('0.0.0.0', 8081)
1. docker ps -a [search name for that port]
2. docker stop airflow_yt-webserver-1

## whats airflow dag catup and backfill?
catchup=True (default) tells Airflow to run all past DAG runs from the start_date up to the current date.

If set to False, Airflow only runs the most recent schedule (ignores past runs).

##  Backfill (Manual CLI command) [Manually run missed DAG runs]
[[[A command-line operation to manually run a DAG for a past date range.]

    Useful for reprocessing historical data or rerunning missed DAGs.

[[[airflow dags backfill -s 2024-01-01 -e 2024-01-10 my_dag

This will run the DAG my_dag for each day between Jan 1 and Jan 10, 2024.]

### if want to start fresh
# 1. (Optional) Clean everything — only if starting fresh [erase all]
docker compose down --volumes --remove-orphans

# 2. Initialize DB & create admin user
docker compose run --rm airflow-init bash -c "airflow db reset -y && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"

# 3. Start all services
docker compose up -d

# 4. Visit Airflow UI
#    http://localhost:8081

## after creating user using [docker compose run --rm airflow-init bash -c "airflow db reset -y && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"]

docker compose up -d

## bash operator documentation link

"https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html

## after logging on computer

1. docker compose run --rm airflow-init bash -c "airflow db reset -y && airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
2. docker compose up -d

### If changes are made in docker-compose.yml or Dockerfile:

1. Stop and remove all containers and volumes (important if DB structure changed)
docker compose down -v

2. Rebuild everything based on the new config and Dockerfile
docker compose up --build -d

3. (Optional but recommended) Reset DB and create an admin user
docker compose run --rm airflow-init bash -c "airflow db reset -y && \
airflow users create --username admin --password admin \
--firstname Admin --lastname User --role Admin --email admin@example.com"

4. Start all services in detached mode
docker compose up -d

## get a shell prompt inside that specific container =>[docker exec -it e949414e06aa bash]  => container id
[[[is used to open an interactive shell (bash) inside a running Docker container.]]

From there, you can:

    Check/install packages (pip install ...)

    Run Python (python)

    Inspect Airflow directories and logs

    Debug DAG or environment issues

## docker-compose up --build

## generate fernet keys
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

## generate secrete key
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
