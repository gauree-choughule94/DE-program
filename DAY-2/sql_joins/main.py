from db_connection import get_db_connection
from faker import Faker
import random


def create_tables_and_insert_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    fake = Faker()

    cursor.execute("DROP TABLE IF EXISTS self_employees")
    cursor.execute("DROP TABLE IF EXISTS projects1")
    cursor.execute("DROP TABLE IF EXISTS clients1")

    # Create self_employees table with self-join support
    cursor.execute("""
        CREATE TABLE self_employees (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            service VARCHAR(100),
            manager_id INT,
            FOREIGN KEY (manager_id) REFERENCES self_employees(id)
        );
    """)


    # Insert 4 employees as managers (no manager_id)
    managers = []
    for i in range(1, 5):
        managers.append((i, fake.name(), fake.email(), fake.job(), None))
    cursor.executemany("INSERT INTO self_employees (id, name, email, service, manager_id) VALUES (%s, %s, %s, %s, %s)", managers)

    # Insert 45 employees and assign each a random manager from the first 4
    self_employees = []
    for i in range(5, 51):
        manager_id = random.randint(1, 4)
        self_employees.append((i, fake.name(), fake.email(), fake.job(), manager_id))
    cursor.executemany("INSERT INTO self_employees (id, name, email, service, manager_id) VALUES (%s, %s, %s, %s, %s)", self_employees)


     # Create clients1 table
    cursor.execute("""
        CREATE TABLE clients1 (
            id INT PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            country VARCHAR(50)
        )
    """)

    # Create projects1 table
    cursor.execute("""
        CREATE TABLE projects1 (
            id INT PRIMARY KEY,
            title VARCHAR(100),
            manager_email VARCHAR(100),
            client_id INT,
            FOREIGN KEY (client_id) REFERENCES clients1(id)
        )
    """)

    # Insert data into clients1 with duplicates
    clients_data = [
        (1, 'Alice Smith', 'alice@example.com', 'USA'),
        (2, 'Bob Johnson', 'bob@example.com', 'Canada'),
        (3, 'Charlie Lee', 'charlie@example.com', 'UK'),
        (4, 'Dana White', 'dana@example.com', 'Germany'),
        (5, 'Eva Green', 'eva@example.com', 'France'),
        (6, 'Frank Black', 'frank@example.com', 'USA'),
        (7, 'Grace Blue', 'grace@example.com', 'Italy'),
        (8, 'Henry Gold', 'henry@example.com', 'Canada'),
        (9, 'Isla Brown', 'isla@example.com', 'UK'),
        (10, 'Jack Red', 'jack@example.com', 'Australia'),
    ]
    cursor.executemany("INSERT INTO clients1 (id, name, email, country) VALUES (%s, %s, %s, %s)", clients_data)

    # Insert data into projects1 with duplicate title & email matching clients
    projects_data = [
        (1, 'Alice Smith', 'alice@example.com', 1),            # Duplicate of client 1
        (2, 'HR Automation', 'm2@corp.com', 2),
        (3, 'Charlie Lee', 'charlie@example.com', 3),          # Duplicate of client 3
        (4, 'CRM Integration', 'm4@corp.com', None),
        (5, 'Mobile App', 'm5@corp.com', 4),
        (6, 'Eva Green', 'eva@example.com', None),             # Duplicate of client 5
        (7, 'ERP Development', 'm7@corp.com', 5),
        (8, 'Security Upgrade', 'm8@corp.com', 6),
        (9, 'Cloud Migration', 'm9@corp.com', 2),
        (10, 'Chatbot Assistant', 'm10@corp.com', 7),
    ]
    cursor.executemany("INSERT INTO projects1 (id, title, manager_email, client_id) VALUES (%s, %s, %s, %s)", projects_data)

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created and data inserted successfully.")

if __name__ == "__main__":
    create_tables_and_insert_data()
