-- Create table
CREATE TABLE employees (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    department_id INT,
    salary DECIMAL(10, 2)
);

-- Create table
CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

-- Insert sample data
INSERT INTO departments (id, name) VALUES (1, 'HR'), (2, 'IT'), (3, 'Sales');

INSERT INTO employees (id, name, department_id, salary) VALUES
(1, 'Alice', 1, 5000),
(2, 'Bob', 1, 5200),
(3, 'Charlie', 2, 6100),
(4, 'Diana', 2, 6200),
(5, 'Eve', 3, 5800),
(6, 'John', 2, 10000),
(7, 'Bolby', 3, 5500),
(8, 'Ketki', 3, 4500),
(9, 'Ron', 2, 6150),
(10, 'Jane', 2, 6100);

use my_db1;
select name, department_id, salary, rank() over (partition by department_id order by salary desc) 
as rank1 from employees;
"""
# name, department_id, salary, rank1
'Bob', '1', '5200.00', '1'
'Alice', '1', '5000.00', '2'
'John', '2', '10000.00', '1'
'Diana', '2', '6200.00', '2'
'Ron', '2', '6150.00', '3'
'Charlie', '2', '6100.00', '4'
'Jane', '2', '6100.00', '4'
'Eve', '3', '5800.00', '1'
'Bolby', '3', '5500.00', '2'
'Ketki', '3', '4500.00', '3'

"""

select name, department_id, salary, rank() over (partition by department_id order by salary desc) 
as rank1, dense_rank() over (partition by department_id order by salary desc) 
as rank_dense from employees;

"""
# name, department_id, salary, rank1, rank_dense
'Bob', '1', '5200.00', '1', '1'
'Alice', '1', '5000.00', '2', '2'
'John', '2', '10000.00', '1', '1'
'Diana', '2', '6200.00', '2', '2'
'Ron', '2', '6150.00', '3', '3'
'Charlie', '2', '6100.00', '4', '4'
'Jane', '2', '6100.00', '4', '4'
'Eve', '3', '5800.00', '1', '1'
'Bolby', '3', '5500.00', '2', '2'
'Ketki', '3', '4500.00', '3', '3'

"""

select name, department_id, salary, rank() over (order by salary desc) 
as rank1, dense_rank() over (order by salary desc) as rank_dense from employees; -- correct

"""
# name, department_id, salary, rank1, rank_dense
'John', '2', '10000.00', '1', '1'
'SALMAN', '2', '9500.00', '2', '2'
'ZAYN', '3', '7200.00', '3', '3'
'Diana', '2', '6200.00', '4', '4'
'Ron', '2', '6150.00', '5', '5'
'Charlie', '2', '6100.00', '6', '6'
'Jane', '2', '6100.00', '6', '6'
'Eve', '3', '5800.00', '8', '7'
'Bolby', '3', '5500.00', '9', '8'
'Bob', '1', '5200.00', '10', '9'
'Alice', '1', '5000.00', '11', '10'
'Ketki', '3', '4500.00', '12', '11'

"""


select name, department_id, salary, lead(salary) over() as next_sal, lag(salary) over() 
as prev_sal from employees;
"""
# name, department_id, salary, next_sal, prev_sal
'Alice', '1', '5000.00', '5200.00', NULL
'Bob', '1', '5200.00', '6100.00', '5000.00'
'Charlie', '2', '6100.00', '6200.00', '5200.00'
'Diana', '2', '6200.00', '5800.00', '6100.00'
'Eve', '3', '5800.00', '10000.00', '6200.00'
'John', '2', '10000.00', '5500.00', '5800.00'
'Bolby', '3', '5500.00', '4500.00', '10000.00'
'Ketki', '3', '4500.00', '6150.00', '5500.00'
'Ron', '2', '6150.00', '6100.00', '4500.00'
'Jane', '2', '6100.00', '9500.00', '6150.00'
'SALMAN', '2', '9500.00', '7200.00', '6100.00'
'ZAYN', '3', '7200.00', NULL, '9500.00'

"""

select name, department_id, salary, lead(salary) over(order by salary desc) as next_sal, lag(salary) 
over(order by salary desc) as prev_sal from employees;    
"""
# name, department_id, salary, next_sal, prev_sal
'John', '2', '10000.00', '9500.00', NULL
'SALMAN', '2', '9500.00', '7200.00', '10000.00'
'ZAYN', '3', '7200.00', '6200.00', '9500.00'
'Diana', '2', '6200.00', '6150.00', '7200.00'
'Ron', '2', '6150.00', '6100.00', '6200.00'
'Charlie', '2', '6100.00', '6100.00', '6150.00'
'Jane', '2', '6100.00', '5800.00', '6100.00'
'Eve', '3', '5800.00', '5500.00', '6100.00'
'Bolby', '3', '5500.00', '5200.00', '5800.00'
'Bob', '1', '5200.00', '5000.00', '5500.00'
'Alice', '1', '5000.00', '4500.00', '5200.00'
'Ketki', '3', '4500.00', NULL, '5000.00'

"""

-- same no difference
select name, department_id, salary, lead(salary, 0) over(order by salary desc) 
as next_sal, lag(salary, 0) over(order by salary desc) as prev_sal from employees;
"""
# name	department_id	salary	next_sal	prev_sal
'John', '2', '10000.00', '10000.00', '10000.00'
'SALMAN', '2', '9500.00', '9500.00', '9500.00'
'ZAYN', '3', '7200.00', '7200.00', '7200.00'
'Diana', '2', '6200.00', '6200.00', '6200.00'
'Ron', '2', '6150.00', '6150.00', '6150.00'
'Charlie', '2', '6100.00', '6100.00', '6100.00'
'Jane', '2', '6100.00', '6100.00', '6100.00'
'Eve', '3', '5800.00', '5800.00', '5800.00'
'Bolby', '3', '5500.00', '5500.00', '5500.00'
'Bob', '1', '5200.00', '5200.00', '5200.00'
'Alice', '1', '5000.00', '5000.00', '5000.00'
'Ketki', '3', '4500.00', '4500.00', '4500.00'

"""

select name, department_id, salary, lead(salary) over(partition by department_id order by salary desc) 
as next_sal, lag(salary) over(partition by department_id order by salary desc) as prev_sal from employees;

"""
# name, department_id, salary, next_sal, prev_sal
'Bob', '1', '5200.00', '5000.00', NULL
'Alice', '1', '5000.00', NULL, '5200.00'
'John', '2', '10000.00', '9500.00', NULL
'SALMAN', '2', '9500.00', '6200.00', '10000.00'
'Diana', '2', '6200.00', '6150.00', '9500.00'
'Ron', '2', '6150.00', '6100.00', '6200.00'
'Charlie', '2', '6100.00', '6100.00', '6150.00'
'Jane', '2', '6100.00', NULL, '6100.00'
'ZAYN', '3', '7200.00', '5800.00', NULL
'Eve', '3', '5800.00', '5500.00', '7200.00'
'Bolby', '3', '5500.00', '4500.00', '5800.00'
'Ketki', '3', '4500.00', NULL, '5500.00'

"""
select name, department_id, salary, lead(salary) over(order by salary desc) 
as next_sal, lag(salary) over(order by salary desc) as prev_sal from employees;

"""
# name, department_id, salary, next_sal, prev_sal
'John', '2', '10000.00', '9500.00', NULL
'SALMAN', '2', '9500.00', '7200.00', '10000.00'
'ZAYN', '3', '7200.00', '6200.00', '9500.00'
'Diana', '2', '6200.00', '6150.00', '7200.00'
'Ron', '2', '6150.00', '6100.00', '6200.00'
'Charlie', '2', '6100.00', '6100.00', '6150.00'
'Jane', '2', '6100.00', '5800.00', '6100.00'
'Eve', '3', '5800.00', '5500.00', '6100.00'
'Bolby', '3', '5500.00', '5200.00', '5800.00'
'Bob', '1', '5200.00', '5000.00', '5500.00'
'Alice', '1', '5000.00', '4500.00', '5200.00'
'Ketki', '3', '4500.00', NULL, '5000.00'

"""

select name, department_id, salary, lead(salary, 2, 0) over(order by salary desc) 
as next_sal, lag(salary, 2, 0) over(order by salary desc) as prev_sal from employees;
"""
# name, department_id, salary, next_sal, prev_sal
'John', '2', '10000.00', '7200.00', '0.00'
'SALMAN', '2', '9500.00', '6200.00', '0.00'
'ZAYN', '3', '7200.00', '6150.00', '10000.00'
'Diana', '2', '6200.00', '6100.00', '9500.00'
'Ron', '2', '6150.00', '6100.00', '7200.00'
'Charlie', '2', '6100.00', '5800.00', '6200.00'
'Jane', '2', '6100.00', '5500.00', '6150.00'
'Eve', '3', '5800.00', '5200.00', '6100.00'
'Bolby', '3', '5500.00', '5000.00', '6100.00'
'Bob', '1', '5200.00', '4500.00', '5800.00'
'Alice', '1', '5000.00', '0.00', '5500.00'
'Ketki', '3', '4500.00', '0.00', '5200.00'

"""

select name, department_id, salary, row_number() over() as r_n from employees;

"""
# name, department_id, salary, r_n
'Alice', '1', '5000.00', '1'
'Bob', '1', '5200.00', '2'
'Charlie', '2', '6100.00', '3'
'Diana', '2', '6200.00', '4'
'Eve', '3', '5800.00', '5'
'John', '2', '10000.00', '6'
'Bolby', '3', '5500.00', '7'
'Ketki', '3', '4500.00', '8'
'Ron', '2', '6150.00', '9'
'Jane', '2', '6100.00', '10'
'SALMAN', '2', '9500.00', '11'
'ZAYN', '3', '7200.00', '12'

"""
select name, department_id, salary, row_number() over(order by salary) as r_n from employees;

"""
# name, department_id, salary, r_n
'Ketki', '3', '4500.00', '1'
'Alice', '1', '5000.00', '2'
'Bob', '1', '5200.00', '3'
'Bolby', '3', '5500.00', '4'
'Eve', '3', '5800.00', '5'
'Charlie', '2', '6100.00', '6'
'Jane', '2', '6100.00', '7'
'Ron', '2', '6150.00', '8'
'Diana', '2', '6200.00', '9'
'ZAYN', '3', '7200.00', '10'
'SALMAN', '2', '9500.00', '11'
'John', '2', '10000.00', '12'

"""
select name, department_id, salary, row_number() over(partition by department_id) as r_n from employees; -- wrong

select top 4 * from employees;     -- sql server only
select * from employees limit 4;
select distinct salary from employees order by salary desc limit 2, 1;

select name, department_id, salary, first_value(salary) over(order by salary) as salryyy from employees;
"""
# name, department_id, salary, salryyy
'Ketki', '3', '4500.00', '4500.00'
'Alice', '1', '5000.00', '4500.00'
'Bob', '1', '5200.00', '4500.00'
'Bolby', '3', '5500.00', '4500.00'
'Eve', '3', '5800.00', '4500.00'
'Charlie', '2', '6100.00', '4500.00'
'Jane', '2', '6100.00', '4500.00'
'Ron', '2', '6150.00', '4500.00'
'Diana', '2', '6200.00', '4500.00'
'ZAYN', '3', '7200.00', '4500.00'
'SALMAN', '2', '9500.00', '4500.00'
'John', '2', '10000.00', '4500.00'

"""
select name, department_id, salary, first_value(salary) over(partition by department_id order by salary) 
as salryyy from employees;

"""
# name, department_id, salary, salryyy
'Alice', '1', '5000.00', '5000.00'
'Bob', '1', '5200.00', '5000.00'
'Charlie', '2', '6100.00', '6100.00'
'Jane', '2', '6100.00', '6100.00'
'Ron', '2', '6150.00', '6100.00'
'Diana', '2', '6200.00', '6100.00'
'SALMAN', '2', '9500.00', '6100.00'
'John', '2', '10000.00', '6100.00'
'Ketki', '3', '4500.00', '4500.00'
'Bolby', '3', '5500.00', '4500.00'
'Eve', '3', '5800.00', '4500.00'
'ZAYN', '3', '7200.00', '4500.00'

"""
select name, department_id, salary, first_value(name) over(order by salary) as first_name from employees;
"""
# name, department_id, salary, first_name
'Ketki', '3', '4500.00', 'Ketki'
'Alice', '1', '5000.00', 'Ketki'
'Bob', '1', '5200.00', 'Ketki'
'Bolby', '3', '5500.00', 'Ketki'
'Eve', '3', '5800.00', 'Ketki'
'Charlie', '2', '6100.00', 'Ketki'
'Jane', '2', '6100.00', 'Ketki'
'Ron', '2', '6150.00', 'Ketki'
'Diana', '2', '6200.00', 'Ketki'
'ZAYN', '3', '7200.00', 'Ketki'
'SALMAN', '2', '9500.00', 'Ketki'
'John', '2', '10000.00', 'Ketki'

"""
select name, department_id, salary, first_value(department_id) over(order by salary) as dep_id from employees;
"""
# name, department_id, salary, dep_id
'Ketki', '3', '4500.00', '3'
'Alice', '1', '5000.00', '3'
'Bob', '1', '5200.00', '3'
'Bolby', '3', '5500.00', '3'
'Eve', '3', '5800.00', '3'
'Charlie', '2', '6100.00', '3'
'Jane', '2', '6100.00', '3'
'Ron', '2', '6150.00', '3'
'Diana', '2', '6200.00', '3'
'ZAYN', '3', '7200.00', '3'
'SALMAN', '2', '9500.00', '3'
'John', '2', '10000.00', '3'

"""
select name, department_id, salary, last_value(salary) over (partition by department_id 
order by salary rows between unbounded preceding and unbounded following) as last_val from employees;
"""
# name, department_id, salary, last_val
'Alice', '1', '5000.00', '5200.00'
'Bob', '1', '5200.00', '5200.00'
'Charlie', '2', '6100.00', '10000.00'
'Jane', '2', '6100.00', '10000.00'
'Ron', '2', '6150.00', '10000.00'
'Diana', '2', '6200.00', '10000.00'
'SALMAN', '2', '9500.00', '10000.00'
'John', '2', '10000.00', '10000.00'
'Ketki', '3', '4500.00', '7200.00'
'Bolby', '3', '5500.00', '7200.00'
'Eve', '3', '5800.00', '7200.00'
'ZAYN', '3', '7200.00', '7200.00'

"""
select name, department_id, salary, last_value(salary) over(order by salary rows between unbounded
preceding and unbounded following) as last_val from employees;

select name, salary, nth_value(salary, 3) over(order by salary desc rows between unbounded preceding and 
unbounded following) as 3d_high_sal from employees;

"""
# name, salary, 3d_high_sal
'John', '10000.00', '7200.00'
'SALMAN', '9500.00', '7200.00'
'ZAYN', '7200.00', '7200.00'
'Diana', '6200.00', '7200.00'
'Ron', '6150.00', '7200.00'
'Charlie', '6100.00', '7200.00'
'Jane', '6100.00', '7200.00'
'Eve', '5800.00', '7200.00'
'Bolby', '5500.00', '7200.00'
'Bob', '5200.00', '7200.00'
'Alice', '5000.00', '7200.00'
'Ketki', '4500.00', '7200.00'

"""

select department_id, name, salary, nth_value(salary, 2) over (partition by department_id order by salary desc
rows between unbounded preceding and unbounded following) as second_salary_in_dept from employees;

select name, department_id, salary, ntile(4) over (partition by department_id order by salary desc) 
as salary_quartile from employees;

"""
# name, department_id, salary, salary_quartile
'Bob', '1', '5200.00', '1'
'Alice', '1', '5000.00', '2'
'John', '2', '10000.00', '1'
'SALMAN', '2', '9500.00', '1'
'Diana', '2', '6200.00', '2'
'Ron', '2', '6150.00', '2'
'Charlie', '2', '6100.00', '3'
'Jane', '2', '6100.00', '4'
'ZAYN', '3', '7200.00', '1'
'Eve', '3', '5800.00', '2'
'Bolby', '3', '5500.00', '3'
'Ketki', '3', '4500.00', '4'

"""

select name, department_id, salary, sum(salary) over (partition by department_id) as total_dept_salary
from employees;
"""
# name, department_id, salary, total_dept_salary
'Alice', '1', '5000.00', '10200.00'
'Bob', '1', '5200.00', '10200.00'
'Charlie', '2', '6100.00', '44050.00'
'Diana', '2', '6200.00', '44050.00'
'John', '2', '10000.00', '44050.00'
'Ron', '2', '6150.00', '44050.00'
'Jane', '2', '6100.00', '44050.00'
'SALMAN', '2', '9500.00', '44050.00'
'Eve', '3', '5800.00', '23000.00'
'Bolby', '3', '5500.00', '23000.00'
'Ketki', '3', '4500.00', '23000.00'
'ZAYN', '3', '7200.00', '23000.00'

"""

select name, department_id, salary, avg(salary) over (partition by department_id) as avg_dept_salary
from employees;
"""
# name, department_id, salary, avg_dept_salary
'Alice', '1', '5000.00', '5100.000000'
'Bob', '1', '5200.00', '5100.000000'
'Charlie', '2', '6100.00', '7341.666667'
'Diana', '2', '6200.00', '7341.666667'
'John', '2', '10000.00', '7341.666667'
'Ron', '2', '6150.00', '7341.666667'
'Jane', '2', '6100.00', '7341.666667'
'SALMAN', '2', '9500.00', '7341.666667'
'Eve', '3', '5800.00', '5750.000000'
'Bolby', '3', '5500.00', '5750.000000'
'Ketki', '3', '4500.00', '5750.000000'
'ZAYN', '3', '7200.00', '5750.000000'

"""

select name, department_id, salary, round(avg(salary) over (partition by department_id), 2) as avg_dept_salary
from employees;

"""
# name, department_id, salary, avg_dept_salary
'Alice', '1', '5000.00', '5100.00'
'Bob', '1', '5200.00', '5100.00'
'Charlie', '2', '6100.00', '7341.67'
'Diana', '2', '6200.00', '7341.67'
'John', '2', '10000.00', '7341.67'
'Ron', '2', '6150.00', '7341.67'
'Jane', '2', '6100.00', '7341.67'
'SALMAN', '2', '9500.00', '7341.67'
'Eve', '3', '5800.00', '5750.00'
'Bolby', '3', '5500.00', '5750.00'
'Ketki', '3', '4500.00', '5750.00'
'ZAYN', '3', '7200.00', '5750.00'

"""

select name, department_id, salary, min(salary) over (partition by department_id) as min_dept_salary,
max(salary) over (partition by department_id) as max_dept_salary from employees;

"""
# name, department_id, salary, min_dept_salary, max_dept_salary
'Alice', '1', '5000.00', '5000.00', '5200.00'
'Bob', '1', '5200.00', '5000.00', '5200.00'
'Charlie', '2', '6100.00', '6100.00', '10000.00'
'Diana', '2', '6200.00', '6100.00', '10000.00'
'John', '2', '10000.00', '6100.00', '10000.00'
'Ron', '2', '6150.00', '6100.00', '10000.00'
'Jane', '2', '6100.00', '6100.00', '10000.00'
'SALMAN', '2', '9500.00', '6100.00', '10000.00'
'Eve', '3', '5800.00', '4500.00', '7200.00'
'Bolby', '3', '5500.00', '4500.00', '7200.00'
'Ketki', '3', '4500.00', '4500.00', '7200.00'
'ZAYN', '3', '7200.00', '4500.00', '7200.00'

"""

select name, department_id, salary, count(*) over (partition by department_id) as dept_employee_count
from employees;
"""
# name, department_id, salary, dept_employee_count
'Alice', '1', '5000.00', '2'
'Bob', '1', '5200.00', '2'
'Charlie', '2', '6100.00', '6'
'Diana', '2', '6200.00', '6'
'John', '2', '10000.00', '6'
'Ron', '2', '6150.00', '6'
'Jane', '2', '6100.00', '6'
'SALMAN', '2', '9500.00', '6'
'Eve', '3', '5800.00', '4'
'Bolby', '3', '5500.00', '4'
'Ketki', '3', '4500.00', '4'
'ZAYN', '3', '7200.00', '4'

"""
