## Features

1. Manages an employees table.

2. Automatically converts inserted employee names to uppercase via trigger.

3. Backs up deleted employee records into employee_backup table.

4. Executes stored procedures:

    GetAllEmployees() - fetches all employees.

    GetHighSalaryEmployees(salary) - fetches employees earning more than given salary.

5. Views high-salary employees.

6. Transaction management via transactions.sql file.


## Dependencies

pip install -r requirements.txt


## How to Run

Run the main Python script:  python3 run_sql_script.py


## Script Responsibilities

The run_sql_script.py performs the following in order:

-Execute SQL files (stored procedures, views, triggers, and backup table creation).

-Run a transaction from transactions.sql.

-Call stored procedures:

    GetAllEmployees

    GetHighSalaryEmployees(7000)

-Query View high_salary_employees.

-Test trigger by inserting a lowercase employee name.

-Check backup table contents after deletion trigger.

## outputs are added in output.py file