use my_db1;

drop procedure if exists getallemployees;
create procedure getallemployees()
begin
    select * from employees;
end;



drop procedure if exists gethighsalaryemployees;
create procedure gethighsalaryemployees(in min_salary decimal(10,2))
begin
    select * from employees
    where salary > min_salary;
end;
