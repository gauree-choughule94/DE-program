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

    # Remove comments
    sql_script = sql_script.replace('--', '')

    # Split on ; only if it’s not inside a BEGIN...END block
    statements = []
    statement = ""
    inside_begin_end = False

    for line in sql_script.splitlines():
        stripped = line.strip()

        if stripped.upper().startswith("CREATE TRIGGER") or "BEGIN" in stripped.upper():
            inside_begin_end = True

        statement += line + "\n"

        if ";" in stripped and not inside_begin_end:
            statements.append(statement.strip())
            statement = ""

        if "END;" in stripped.upper():
            inside_begin_end = False
            statements.append(statement.strip())
            statement = ""

    conn = get_db_connection()
    cursor = conn.cursor()

    for stmt in statements:
        if stmt:
            try:
                cursor.execute(stmt)
            except mysql.connector.Error as err:
                print(f"\nError executing:\n{stmt}\n{err}\n")

    conn.commit()
    conn.close()
    print(f"Executed SQL from {filename} successfully.")


def call_stored_procedure():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.callproc('GetAllEmployees')  # Call SP
    
    for result in cursor.stored_results():
        for row in result.fetchall():
            print(row)
    
    conn.close()


def call_high_sal_stored_procedure():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.callproc('GetHighSalaryEmployees', [7000])  # Call SP
    
    for result in cursor.stored_results():
        for row in result.fetchall():
            print(row)
    
    conn.close()

def query_view():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM high_salary_employees;")
    
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    conn.close()


def test_trigger():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Insert a new employee with lowercase name to test the trigger
    query = """
        INSERT INTO employees (id, name, department_id, salary)
        VALUES (%s, %s, %s, %s)
    """
    data = (101, "salman", 2, 7500)

    try:
        cursor.execute(query, data)
        conn.commit()
        print("Inserted new employee (trigger should auto-uppercase name).")
    except mysql.connector.Error as err:
        print(f"Error inserting employee:\n{err}")
    finally:
        conn.close()

def check_backup_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM employee_backup;")
    rows = cursor.fetchall()
    
    print("\nBackup Table Contents:")
    for row in rows:
        print(row)
    
    conn.close()


def execute_transaction_sql_file(filename):
    with open(filename, "r") as file:
        sql_script = file.read()

    # Remove SQL-style comments for clarity
    cleaned_script = "\n".join(
        line for line in sql_script.splitlines() if not line.strip().startswith("--")
    )

    statements = [stmt.strip() for stmt in cleaned_script.split(";") if stmt.strip()] # removes extra whitespace, and filters out empty statements.

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        conn.start_transaction()
        print("Transaction started.")

        for stmt in statements:
            print(f"Executing:\n{stmt}")
            cursor.execute(stmt)

        conn.commit()
        print("Transaction committed successfully.")
    except mysql.connector.Error as err:
        conn.rollback()
        print(f"\nTransaction failed. Rolled back.\nError: {err}")
    finally:
        conn.close()


if __name__ == "__main__":
    execute_sql_file("db/sp.sql")
    execute_sql_file("db/views.sql")
    execute_sql_file("db/triggers.sql")
    execute_sql_file("db/backup_table.sql")

    print("\nExecuting Transaction SQL:")
    execute_transaction_sql_file("db/transactions.sql")

    print("\nCalling Stored Procedure:")
    call_stored_procedure()

    print("\nCalling high salary Stored Procedure:")
    call_high_sal_stored_procedure()

    print("\nQuerying View:")
    query_view()

    print("\nInserting new employee to test trigger:")
    test_trigger()

    print("\nChecking Backup Table:")
    check_backup_table()

