use my_db1;

drop view if exists high_salary_employees;
create view high_salary_employees as select * from employees where salary > 6000;
