use my_db1;

start transaction;

-- insert a new employee (should succeed)
insert into employees (id, name, department_id, salary) values (102, 'priti', 3, 6700), (104, 'zayn', 3, 7200);

-- update salary of an existing employee
update employees set salary = salary + 500 where id = 101;

-- -- insert duplicate id to force rollback (this should fail)
-- insert into employees (id, name, department_id, salary) values (104, 'should_fail', 3, 9500);  -- duplicate id will cause error

-- delete an employee (assume id=102 exists)
delete from employees where id = 102;

commit;
