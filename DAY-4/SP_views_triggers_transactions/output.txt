output = """
Executed SQL from db/sp.sql successfully.
Executed SQL from db/views.sql successfully.
Executed SQL from db/triggers.sql successfully.
Executed SQL from db/backup_table.sql successfully.

Executing Transaction SQL:
Transaction started.
Executing:
use my_db1
Executing:
start transaction
Executing:
insert into employees (id, name, department_id, salary) values (102, 'priti', 3, 6700), (104, 'zayn', 3, 7200)

Transaction failed. Rolled back.
Error: 1062 (23000): Duplicate entry '104' for key 'employees.PRIMARY'

Calling Stored Procedure:
(1, 'Alice', 1, Decimal('5000.00'))
(2, 'Bob', 1, Decimal('5200.00'))
(3, 'Charlie', 2, Decimal('6100.00'))
(4, 'Diana', 2, Decimal('6200.00'))
(5, 'Eve', 3, Decimal('5800.00'))
(6, 'John', 2, Decimal('10000.00'))
(7, 'Bolby', 3, Decimal('5500.00'))
(8, 'Ketki', 3, Decimal('4500.00'))
(9, 'Ron', 2, Decimal('6150.00'))
(10, 'Jane', 2, Decimal('6100.00'))
(101, 'SALMAN', 2, Decimal('9500.00'))
(104, 'ZAYN', 3, Decimal('7200.00'))

Calling high salary Stored Procedure:
(6, 'John', 2, Decimal('10000.00'))
(101, 'SALMAN', 2, Decimal('9500.00'))
(104, 'ZAYN', 3, Decimal('7200.00'))

Querying View:
(3, 'Charlie', 2, Decimal('6100.00'))
(4, 'Diana', 2, Decimal('6200.00'))
(6, 'John', 2, Decimal('10000.00'))
(9, 'Ron', 2, Decimal('6150.00'))
(10, 'Jane', 2, Decimal('6100.00'))
(101, 'SALMAN', 2, Decimal('9500.00'))
(104, 'ZAYN', 3, Decimal('7200.00'))

Inserting new employee to test trigger:
Error inserting employee:
1062 (23000): Duplicate entry '101' for key 'employees.PRIMARY'

Checking Backup Table:

Backup Table Contents:
(102, 'PRITI', 3, Decimal('6700.00'), datetime.datetime(2025, 4, 15, 15, 45, 21))
"""