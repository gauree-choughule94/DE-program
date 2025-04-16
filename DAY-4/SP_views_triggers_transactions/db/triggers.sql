use my_db1;

drop trigger if exists before_employee_insert;

create trigger before_employee_insert
before insert on employees
for each row
begin
    set new.name = upper(new.name);
end;


drop trigger if exists before_employee_delete;

create trigger before_employee_delete
before delete on employees
for each row
begin
    insert into employee_backup (id, name, department_id, salary)
    values (old.id, old.name, old.department_id, old.salary);
end;