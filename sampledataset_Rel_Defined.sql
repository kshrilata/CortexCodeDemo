--To demonstrate an Entity Relationship Diagram (ERD) in Snowflake, we need to cover the four primary relationship types: One-to-One (1:1), One-to-Many (1:1:M), Many-to-Many (M:M), and Self-Referencing (Recursive).

--Below is a complete script to build a "Corporate Ecosystem" schema that illustrates these concepts.

--1. The Schema Setup
--First, we create a clean environment to house our demonstration tables.

--SQL
CREATE OR REPLACE DATABASE erd_demo_db;
CREATE OR REPLACE SCHEMA corporate_schema;
USE SCHEMA corporate_schema;
--2. One-to-One (1:1) Relationship
--In a 1:1 relationship, each record in Table A relates to exactly one record in Table B. This is common for sensitive data or "extensions" of a profile.

--Example: An EMPLOYEE has exactly one BACKGROUND_CHECK.

--SQL
-- Parent Table
CREATE OR REPLACE TABLE employees (
    employee_id INT PRIMARY KEY,
    full_name STRING,
    hire_date DATE
);

-- Child Table (1:1)
CREATE OR REPLACE TABLE background_checks (
    check_id INT PRIMARY KEY,
    employee_id INT UNIQUE NOT NULL, -- UNIQUE ensures 1:1
    status STRING,
    clearance_date DATE,
    CONSTRAINT fk_employee_check FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

INSERT INTO employees VALUES (101, 'Alice Smith', '2023-01-15'), (102, 'Bob Jones', '2023-03-20');
INSERT INTO background_checks VALUES (1, 101, 'Cleared', '2023-01-10'), (2, 102, 'Pending', NULL);

--3. One-to-Many (1:M) Relationship
--This is the most common relationship. One record in the parent table relates to multiple records in the child table.

--Example: One DEPARTMENT has many EMPLOYEES.

--SQL
-- Parent Table
CREATE OR REPLACE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name STRING
);

-- Add Dept ID to Employees to create 1:M
ALTER TABLE employees ADD COLUMN dept_id INT;
ALTER TABLE employees ADD CONSTRAINT fk_dept FOREIGN KEY (dept_id) REFERENCES departments(dept_id);

INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing');
UPDATE employees SET dept_id = 1 WHERE employee_id = 101;
UPDATE employees SET dept_id = 2 WHERE employee_id = 102;
--4. Many-to-Many (M:M) Relationship
--In Snowflake (and relational DBs), M:M relationships require a Join Table (or Associative Table).

--Example: EMPLOYEES work on many PROJECTS, and PROJECTS have many EMPLOYEES.

--SQL
-- The Project Table
CREATE OR REPLACE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name STRING,
    budget DECIMAL(15,2)
);

-- The Join Table (Associative Table)
CREATE OR REPLACE TABLE employee_project_assignment (
    employee_id INT,
    project_id INT,
    role_assigned STRING,
    PRIMARY KEY (employee_id, project_id),
    CONSTRAINT fk_assignment_emp FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    CONSTRAINT fk_assignment_proj FOREIGN KEY (project_id) REFERENCES projects(project_id)
);

INSERT INTO projects VALUES (500, 'Cloud Migration', 100000), (501, 'AI Implementation', 250000);
INSERT INTO employee_project_assignment VALUES (101, 500, 'Lead Developer'), (101, 501, 'Consultant'), (102, 500, 'Project Manager');
--5. Self-Referencing (Recursive) Relationship
--This occurs when a table has a foreign key that points back to its own primary key.

--Example: An EMPLOYEE reports to a MANAGER (who is also an employee).

--SQL
-- Add Manager ID to Employees
ALTER TABLE employees ADD COLUMN manager_id INT;
ALTER TABLE employees ADD CONSTRAINT fk_manager FOREIGN KEY (manager_id) REFERENCES employees(employee_id);

-- Alice (101) reports to Bob (102)
UPDATE employees SET manager_id = 102 WHERE employee_id = 101;
