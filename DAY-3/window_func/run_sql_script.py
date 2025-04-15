import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv() 

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

def execute_sql_file(filename):
    with open(filename, "r") as file:
        sql_script = file.read()

    conn = get_db_connection()
    cursor = conn.cursor()
    
    for statement in sql_script.strip().split(';'):
        stmt = statement.strip()
        if stmt:
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print(f"Error executing statement:\n{stmt}\n{err}")
    
    conn.commit()
    conn.close()
    print(f"Executed SQL from {filename} successfully.")

if __name__ == "__main__":
    execute_sql_file("window.sql")
