--- Create Keyspace
CREATE KEYSPACE test WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 3};
USE test;

//1. List the empno,ename,jobtitle,and hiredate of employee from the employee table.
//create table
CREATE TABLE employees (empno INT PRIMARY KEY, ename TEXT, jobtitle TEXT, hiredate DATE);
//insert employees
INSERT INTO employees (empno, name, hiredate, jobtitle, salary) VALUES (1, 'Walter', '2018-06-04', 'HR', 1000);
INSERT INTO employees (empno, name, hiredate, jobtitle, salary) VALUES (2, 'Pranoop', '2018-02-14', 'Clerk', 1500);
INSERT INTO employees (empno, name, hiredate, jobtitle, salary) VALUES (3, 'Nate', '1980-12-17', 'Clerk', 2000);
//select all data from employees, except salary
SELECT empno, ename, jobtitle, hiredate FROM employees;

//2. List the name,salary of the employees who are clerks.
//select salary for Clerks
SELECT name, salary FROM employees WHERE jobtitle = 'Clerk' ALLOW FILTERING;


//3. List the name,job,salary of every employee joined on ‘december 17,1980’,
//select for employees who joined on specific date
SELECT name, jobtitle, salary FROM employees WHERE hiredate = '1980-12-17' ALLOW FILTERING;

//4. List name and annual salary of all the employees.
//Select name and salary from all employees
SELECT name, salary FROM employees;

//5. List the department name & deptno for departments having deptno.>=20
//alter table to add department number and name
ALTER TABLE employees ADD depno INT;
ALTER TABLE employees ADD depname TEXT;
//update our entries to add deptno and deptname
UPDATE employees SET depno = 20, depname = 'Sales' WHERE empno = 1;
UPDATE employees SET depno = 1, depname = 'Marketing' WHERE empno = 2;
UPDATE employees SET depno = 2, depname = 'Management' WHERE empno = 3;
SELECT depname, depno FROM employees WHERE depno >= 20 ALLOW FILTERING;

//6. Display employees’ names, salary and manager values of those employees whose salary is 500 from EMP table using SELECT statement.
//alter table to add mngno
ALTER TABLE employees ADD mngno INT;
//update our entries to include mngno
UPDATE employees SET mngno = 0 WHERE empno = 1;
UPDATE employees SET mngno = 2 WHERE empno = 2;
UPDATE employees SET mngno = 3 WHERE empno = 3;
SELECT ename, salary, mngno FROM employees WHERE salary = 500 ALLOW FILTERING;

//7. Write a query that displays enames with first letter capitalized and all other letters 
//   in lowercase and length of enames whose enames start with J, A or N.
//Create a function to get length
CREATE FUNCTION IF NOT EXISTS len (input text) 
   CALLED ON NULL INPUT 
   RETURNS int 
   LANGUAGE java AS '
   return input.length();';
//select ename and ename length if employees name begins with J, A or N
SELECT ename, len(ename) FROM employees WHERE name CONTAINS "J" , "A", "N"