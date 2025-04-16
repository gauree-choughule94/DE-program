from db_connection import get_db_connection
import pandas as pd

def run_joins_and_save():
    conn = get_db_connection()
    cursor = conn.cursor()

    joins = {
        "inner_join.csv": """
            select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
            from clients1
            inner join projects1 on clients1.id = projects1.client_id;
        """,

        "left_join.csv": """
            select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
            from clients1
            left join projects1 on clients1.id = projects1.client_id;
        """,

        "right_join.csv": """
            select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
            from clients1
            right join projects1 on clients1.id = projects1.client_id;
        """,

# select e.name as employee_name
# from employees1 e
# where not exists (
#     select 1
#     from departments1 d
#     where e.department_id = d.id
# );
        "left_anti_join.csv": """
            select clients1.name as client_name, clients1.email
            from clients1
            left join projects1 on clients1.id = projects1.client_id
            where projects1.client_id is null;
        """,

        "right_anti_join.csv": """
            select projects1.title as project_title, projects1.manager_email
            from clients1
            right join projects1 on clients1.id = projects1.client_id
            where clients1.id is null;
        """,

        "full_outer_join.csv": """
            select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
            from clients1
            left join projects1 on clients1.id = projects1.client_id
            union
            select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
            from clients1
            right join projects1 on clients1.id = projects1.client_id;
        """,

        "union.csv": """
            select name, email from clients1
            union
            select title, manager_email from projects1;
        """,

        "union_all.csv": """
            select name, email from clients1
            union all
            select title, manager_email from projects1;
        """,

        "cross_join.csv": """
            select clients1.name as client_name, projects1.title as project_title
            from clients1
            cross join projects1;
        """
    }

    for filename, query in joins.items():
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.dataframe(rows, columns=columns)
        df.to_csv(filename, index=False)
        print(f"{filename} saved.")

    cursor.close()
    conn.close()


def run_self_join_and_save():
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        select 
            e.name as employee_name,
            e.service as employee_service,
            m.name as manager_name,
            m.service as manager_service
        from self_employees e
        left join self_employees m on e.manager_id = m.id;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.dataframe(rows, columns=columns)
    df.to_csv("self_join.csv", index=False)
    print("self join saved as self_join.csv")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_joins_and_save()
    run_self_join_and_save()

    