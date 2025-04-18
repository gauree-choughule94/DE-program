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

drop procedure if exists getallemployees_with_count;
create procedure getallemployees_with_count(out employee_count int)
begin
    select * from employees;
    select count(*) into employee_count from employees;
end;


drop procedure if exists gethighsalaryemployees_with_count;
create procedure gethighsalaryemployees_with_count(
    in min_salary decimal(10,2),
    out employee_count int
)
begin
    select * from employees
    where salary > min_salary;

    select count(*) into employee_count 
    from employees
    where salary > min_salary;
end;