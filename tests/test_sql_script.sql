-- Create table 1
CREATE TABLE table1 (
    id INT PRIMARY KEY,
    name VARCHAR(50)
);

-- Insert values into table 1
INSERT INTO table1 (id, name) VALUES (1, 'John');
INSERT INTO table1 (id, name) VALUES (2, 'Alice');
INSERT INTO table1 (id, name) VALUES (3, 'Bob');

-- Create table 2
CREATE TABLE table2 (
    id INT PRIMARY KEY,
    age INT
);

-- Insert values into table 2
INSERT INTO table2 (id, age) VALUES (1, 25);
INSERT INTO table2 (id, age) VALUES (2, 30);
INSERT INTO table2 (id, age) VALUES (3, 40);

-- Perform a select query
SELECT * FROM table1;

-- Add a new column to table 1
ALTER TABLE table1 ADD COLUMN email VARCHAR(100);

-- Update values in table 1
UPDATE table1 SET email = 'john@example.com' WHERE id = 1;
UPDATE table1 SET email = 'alice@example.com' WHERE id = 2;
UPDATE table1 SET email = 'bob@example.com' WHERE id = 3;

-- Perform another select query
SELECT * FROM table1;

-- Drop a column from table 1
ALTER TABLE table1 DROP COLUMN email;

-- Perform a final select query
SELECT * FROM table1;
