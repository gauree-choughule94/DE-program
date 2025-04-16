use my_db1;

CREATE TABLE IF NOT EXISTS employee_backup (
    id INT,
    name VARCHAR(100),
    department_id INT,
    salary DECIMAL(10, 2),
    deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
