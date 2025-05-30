docker-compose up -d airflow-init  => pulling images

docker-compose up --build
#####or

docker-compose up -d --build

docker-compose down -v   

docker-compose up -d

docker-compose restart


##  ERROR: You need to initialize the database
1. docker exec -it 223821a7951d bash > airflow db init 

2. docker-compose restart or docker-compose down -v

3. docker-compose up --build  
docker commit cb4716791acc gauree11/ubuntu-custom:latest


### ## generate fernet keys
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

## generate secrete key
python3 -c 'import secrets; print(secrets.token_urlsafe(32))'

docker exec -it e530febe3566 /bin/bash
python3 /opt/airflow/scripts/extract_mysql.py 2025-05-19T00:00:00 /opt/airflow/data/mysql_data.csv


day-27(main folder)
    config
        ...
    data
    dags
        incre.....py
    script
        extract_mysql.py
    notebooks
        run_etl.ipynb
    mysql
        init.sql
    Dockerfile
    docker-compose.yaml

    echo "docker@1234" | docker login --username gauree11 --password-stdin

echo "docker@1234" | docker login --username gauree11 --password-stdin => if not work then folow below
gpg --full-generate-key
pass init "gauree.choughule@neosoftmail.com"
echo "docker@1234" | docker login --username gauree11 --password- => Login Succeeded


###### ETL

Component	=> Purpose
extract_mysql.py	=> Extracts new/updated MySQL users table rows and writes to a CSV.
last_run.txt	=> Stores last successful extraction timestamp (in the script).
Airflow DAG =>	Orchestrates the incremental ETL flow.
Airflow Variable =>	Stores a centralized last_updated timestamp across runs.
extract_mysql() in DAG	=> Calls extract_mysql.py, updates the Airflow Variable with new time.
extract_api() in DAG	=> Uses same timestamp to fetch updated data from an external API.


result.stdout captures all the print() output from extract_mysql.py

result.stderr captures any errors or warnings the script printed


match = re.search(r"\[RESULT\]\s*(\d{4}-\d{2}-\d{2}T[^\s]+)", result.stdout)
if match:
    new_timestamp = match.group(1)
    Variable.set("last_updated", new_timestamp)         ### searches the script’s output for a line like:

[RESULT] 2025-05-27T07:10:47.134255+00:00

Then it:

    Extracts the timestamp

    Saves it as an Airflow variable (Variable.set("last_updated", new_timestamp))



### jar
The jar (Java ARchive) is needed in your setup because Spark uses JDBC to connect to MySQL, and that connection requires the MySQL JDBC driver, which is a .jar file (typically mysql-connector-java-x.x.x.jar).

### capture_output=True, text=True, check=True
Capturing and printing its output

Getting the output as text (not bytes) => (text=True)

Failing fast if the script fails (check=True)