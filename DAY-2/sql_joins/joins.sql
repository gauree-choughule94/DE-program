-- inner join
select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
from clients1
inner join projects1 on clients1.id = projects1.client_id;

"""
client_name,project_title,manager_email
Alice Smith,Alice Smith,alice@example.com
Bob Johnson,HR Automation,m2@corp.com
Charlie Lee,Charlie Lee,charlie@example.com
Dana White,Mobile App,m5@corp.com
Eva Green,ERP Development,m7@corp.com
Frank Black,Security Upgrade,m8@corp.com
Bob Johnson,Cloud Migration,m9@corp.com
Grace Blue,Chatbot Assistant,m10@corp.com
"""

-- left join
select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
from clients1
left join projects1 on clients1.id = projects1.client_id;

"""
client_name,project_title,manager_email
Alice Smith,Alice Smith,alice@example.com
Bob Johnson,HR Automation,m2@corp.com
Bob Johnson,Cloud Migration,m9@corp.com
Charlie Lee,Charlie Lee,charlie@example.com
Dana White,Mobile App,m5@corp.com
Eva Green,ERP Development,m7@corp.com
Frank Black,Security Upgrade,m8@corp.com
Grace Blue,Chatbot Assistant,m10@corp.com
Henry Gold,,
Isla Brown,,
Jack Red,,
"""

-- right join
select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
from clients1
right join projects1 on clients1.id = projects1.client_id;

"""
client_name,project_title,manager_email
Alice Smith,Alice Smith,alice@example.com
Bob Johnson,HR Automation,m2@corp.com
Charlie Lee,Charlie Lee,charlie@example.com
,CRM Integration,m4@corp.com
Dana White,Mobile App,m5@corp.com
,Eva Green,eva@example.com
Eva Green,ERP Development,m7@corp.com
Frank Black,Security Upgrade,m8@corp.com
Bob Johnson,Cloud Migration,m9@corp.com
Grace Blue,Chatbot Assistant,m10@corp.com
"""

--  SELECT e.name AS employee_name
--  FROM employees1 e
--  WHERE NOT EXISTS (
--      SELECT 1
--      FROM departments1 d
--      WHERE e.department_id = d.id
--  );

-- left anti join
select clients1.name as client_name, clients1.email
from clients1
left join projects1 on clients1.id = projects1.client_id
where projects1.client_id is null;

"""
client_name,email
Henry Gold,henry@example.com
Isla Brown,isla@example.com
Jack Red,jack@example.com
"""

-- right anti join
select projects1.title as project_title, projects1.manager_email
from clients1
right join projects1 on clients1.id = projects1.client_id
where clients1.id is null;

"""
project_title,manager_email
CRM Integration,m4@corp.com
Eva Green,eva@example.com
"""

-- full outer join using union
select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
from clients1
left join projects1 on clients1.id = projects1.client_id
union
select clients1.name as client_name, projects1.title as project_title, projects1.manager_email
from clients1
right join projects1 on clients1.id = projects1.client_id;

"""
client_name,project_title,manager_email
Alice Smith,Alice Smith,alice@example.com
Bob Johnson,HR Automation,m2@corp.com
Bob Johnson,Cloud Migration,m9@corp.com
Charlie Lee,Charlie Lee,charlie@example.com
Dana White,Mobile App,m5@corp.com
Eva Green,ERP Development,m7@corp.com
Frank Black,Security Upgrade,m8@corp.com
Grace Blue,Chatbot Assistant,m10@corp.com
Henry Gold,,
Isla Brown,,
Jack Red,,
,CRM Integration,m4@corp.com
,Eva Green,eva@example.com
"""

-- union
select name, email from clients1
union
select title, manager_email from projects1;

"""
name,email
Alice Smith,alice@example.com
Bob Johnson,bob@example.com
Charlie Lee,charlie@example.com
Dana White,dana@example.com
Eva Green,eva@example.com
Frank Black,frank@example.com
Grace Blue,grace@example.com
Henry Gold,henry@example.com
Isla Brown,isla@example.com
Jack Red,jack@example.com
HR Automation,m2@corp.com
CRM Integration,m4@corp.com
Mobile App,m5@corp.com
ERP Development,m7@corp.com
Security Upgrade,m8@corp.com
Cloud Migration,m9@corp.com
Chatbot Assistant,m10@corp.com
"""

-- union all
select name, email from clients1
union all
select title, manager_email from projects1;

"""
name,email
Alice Smith,alice@example.com
Bob Johnson,bob@example.com
Charlie Lee,charlie@example.com
Dana White,dana@example.com
Eva Green,eva@example.com
Frank Black,frank@example.com
Grace Blue,grace@example.com
Henry Gold,henry@example.com
Isla Brown,isla@example.com
Jack Red,jack@example.com
Alice Smith,alice@example.com
HR Automation,m2@corp.com
Charlie Lee,charlie@example.com
CRM Integration,m4@corp.com
Mobile App,m5@corp.com
Eva Green,eva@example.com
ERP Development,m7@corp.com
Security Upgrade,m8@corp.com
Cloud Migration,m9@corp.com
Chatbot Assistant,m10@corp.com
"""

-- self join
select 
    e.name as employee_name,
    e.service as employee_service,
    m.name as manager_name,
    m.service as manager_service
from self_employees e
left join self_employees m on e.manager_id = m.id;

"""
employee_name,employee_service,manager_name,manager_service
Felicia Banks,Production designer,,
Carrie Lopez,Osteopath,,
Daniel Lowe,Ophthalmologist,,
Holly Clark,Teacher,,
Lori Peterson,Scientist,Felicia Banks,Production designer
Melissa Mclaughlin,Designer,Daniel Lowe,Ophthalmologist
John Barron,Herbalist,Lori Peterson,Scientist
Brandon Baker,Associate Professor,Carrie Lopez,Osteopath
Joshua Nelson,Engineer,Carrie Lopez,Osteopath
Jamie Roman,Armed forces logistics,Felicia Banks,Production designer
Jose Mayo,Teacher,Felicia Banks,Production designer
Michael Johnson,Conservator,Felicia Banks,Production designer
Joshua Rodriguez,Purchasing manager,Daniel Lowe,Ophthalmologist
Christy Green,Psychologist,Holly Clark,Teacher
Leah Lee,Audiological scientist,Holly Clark,Teacher
Rodney Brown,Engineer,Felicia Banks,Production designer
Charles Rangel,Wellsite geologist,Carrie Lopez,Osteopath
Melissa Murphy,Forensic psychologist,Daniel Lowe,Ophthalmologist
Toni Hawkins,Editor,Lori Peterson,Scientist
Michael Gonzalez,Careers information officer,Carrie Lopez,Osteopath
Vickie Hull,Television production assistant,Carrie Lopez,Osteopath
Andrea Cardenas,Geophysical data processor,Felicia Banks,Production designer
Brian Thompson,Lawyer,Daniel Lowe,Ophthalmologist
Karen Brady,Field seismologist,Felicia Banks,Production designer
Kenneth Johnson,Conservator,Lori Peterson,Scientist
Deborah Carroll,Interpreter,Lori Peterson,Scientist
Emily Foley,Surveyor,Daniel Lowe,Ophthalmologist
Roberta Fernandez,Music tutor,Daniel Lowe,Ophthalmologist
Christy Parks,Teaching laboratory technician,Holly Clark,Teacher
John Hamilton,Investment analyst,Felicia Banks,Production designer
Jessica Kane,Dietitian,Carrie Lopez,Osteopath
Yolanda Lambert,Engineer,Carrie Lopez,Osteopath
Joseph Gomez,Lecturer,Carrie Lopez,Osteopath
Paul Fry,Radiographer,Felicia Banks,Production designer
John Lee,Buyer,Holly Clark,Teacher
Micheal Mays,Engineering geologist,Holly Clark,Teacher
Julia Mitchell,Editor,Daniel Lowe,Ophthalmologist
Kimberly Rich,Restaurant manager,Carrie Lopez,Osteopath
Roger Dawson,Amenity horticulturist,Holly Clark,Teacher
Isaac Miller,Trade mark attorney,Daniel Lowe,Ophthalmologist
Jessica Elliott,Seismic interpreter,Felicia Banks,Production designer
Jerry Dixon,Passenger transport manager,Daniel Lowe,Ophthalmologist
Ann Burns,Printmaker,Carrie Lopez,Osteopath
Andrew Johnson,Corporate treasurer,Lori Peterson,Scientist
Terry Cunningham,Psychotherapist,Felicia Banks,Production designer
Dana Williams,Early years teacher,Holly Clark,Teacher
Lauren Jacobson,Outdoor activities,Lori Peterson,Scientist
Kelly Haley,Textile designer,Carrie Lopez,Osteopath
Melinda Hill,Herbalist,Holly Clark,Teacher
Kenneth Jenkins,Optometrist,Felicia Banks,Production designer
"""

-- cross join
select clients1.name as client_name, projects1.title as project_title
from clients1
cross join projects1;

"""
client_name,project_title
Jack Red,Alice Smith
Isla Brown,Alice Smith
Henry Gold,Alice Smith
Grace Blue,Alice Smith
Frank Black,Alice Smith
Eva Green,Alice Smith
Dana White,Alice Smith
Charlie Lee,Alice Smith
Bob Johnson,Alice Smith
Alice Smith,Alice Smith
Jack Red,HR Automation
Isla Brown,HR Automation
Henry Gold,HR Automation
Grace Blue,HR Automation
Frank Black,HR Automation
Eva Green,HR Automation
Dana White,HR Automation
Charlie Lee,HR Automation
Bob Johnson,HR Automation
Alice Smith,HR Automation
Jack Red,Charlie Lee
Isla Brown,Charlie Lee
Henry Gold,Charlie Lee
Grace Blue,Charlie Lee
Frank Black,Charlie Lee
Eva Green,Charlie Lee
Dana White,Charlie Lee
Charlie Lee,Charlie Lee
Bob Johnson,Charlie Lee
Alice Smith,Charlie Lee
Jack Red,CRM Integration
Isla Brown,CRM Integration
Henry Gold,CRM Integration
Grace Blue,CRM Integration
Frank Black,CRM Integration
Eva Green,CRM Integration
Dana White,CRM Integration
Charlie Lee,CRM Integration
Bob Johnson,CRM Integration
Alice Smith,CRM Integration
Jack Red,Mobile App
Isla Brown,Mobile App
Henry Gold,Mobile App
Grace Blue,Mobile App
Frank Black,Mobile App
Eva Green,Mobile App
Dana White,Mobile App
Charlie Lee,Mobile App
Bob Johnson,Mobile App
Alice Smith,Mobile App
Jack Red,Eva Green
Isla Brown,Eva Green
Henry Gold,Eva Green
Grace Blue,Eva Green
Frank Black,Eva Green
Eva Green,Eva Green
Dana White,Eva Green
Charlie Lee,Eva Green
Bob Johnson,Eva Green
Alice Smith,Eva Green
Jack Red,ERP Development
Isla Brown,ERP Development
Henry Gold,ERP Development
Grace Blue,ERP Development
Frank Black,ERP Development
Eva Green,ERP Development
Dana White,ERP Development
Charlie Lee,ERP Development
Bob Johnson,ERP Development
Alice Smith,ERP Development
Jack Red,Security Upgrade
Isla Brown,Security Upgrade
Henry Gold,Security Upgrade
Grace Blue,Security Upgrade
Frank Black,Security Upgrade
Eva Green,Security Upgrade
Dana White,Security Upgrade
Charlie Lee,Security Upgrade
Bob Johnson,Security Upgrade
Alice Smith,Security Upgrade
Jack Red,Cloud Migration
Isla Brown,Cloud Migration
Henry Gold,Cloud Migration
Grace Blue,Cloud Migration
Frank Black,Cloud Migration
Eva Green,Cloud Migration
Dana White,Cloud Migration
Charlie Lee,Cloud Migration
Bob Johnson,Cloud Migration
Alice Smith,Cloud Migration
Jack Red,Chatbot Assistant
Isla Brown,Chatbot Assistant
Henry Gold,Chatbot Assistant
Grace Blue,Chatbot Assistant
Frank Black,Chatbot Assistant
Eva Green,Chatbot Assistant
Dana White,Chatbot Assistant
Charlie Lee,Chatbot Assistant
Bob Johnson,Chatbot Assistant
Alice Smith,Chatbot Assistant
"""

