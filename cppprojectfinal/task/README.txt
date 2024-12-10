# MiniDB - Simple Database Management System

## Project Introduction

MiniDB is a simple database management system implemented in C++ that supports basic SQL operations, including creating databases, creating tables, inserting data, querying data, updating data and deleting data.

## Features

- Database Operations
  - Create database (CREATE DATABASE)
  - Use database (USE)
  - Save database to file
  - Load database from file

- Table Operations
  - Create table (CREATE TABLE) 
  - Drop table (DROP TABLE)
  - Support INTEGER, FLOAT, TEXT data types

- Data Operations
  - Insert data (INSERT INTO)
  - Query data (SELECT)
  - Update data (UPDATE)
  - Delete data (DELETE)
  - Table join query (INNER JOIN)

- Conditional Queries
  - Support WHERE clause
  - Support AND/OR logical operations
  - Support comparison operations (=, <, >, !=)
  - Support expression calculation

## Usage

### Compile

```bash
g++ -o minidb minidb.cpp
```

### Run

```bash
./minidb input.sql output.csv
```

- input.sql: Input file containing SQL commands
- output.csv: Output file for query results

### SQL Command Examples

```sql
-- Create database
CREATE DATABASE testdb;

-- Use database
USE testdb;

-- Create table
CREATE TABLE users (id INTEGER, name TEXT, age INTEGER);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 20);

-- Query data
SELECT * FROM users;

-- Conditional query
SELECT name, age FROM users WHERE age > 18;

-- Update data
UPDATE users SET age = age + 1 WHERE name = 'Alice';

-- Delete data
DELETE FROM users WHERE id = 1;

-- Table join query
SELECT u.name, o.order_id 
FROM users u 
INNER JOIN orders o 
ON u.id = o.user_id;
```

## Data Storage

- Database stored as .db files
- Table structure and data stored in text format
- Support data persistence

## Limitations and Notes

1. No transaction support
2. No index support
3. String data must use single quotes ('')
4. SQL commands must end with semicolon (;)
5. Support basic arithmetic expression calculation
6. Query results output in CSV format

## Project Structure

```
task/
|-- bin/
|   |-- minidb.exe       
|-- src/
|   |-- minidb.cpp     // Main program source code
|-- README.txt        // Documentation
```

## Main Classes

- `Database`: Database class, manages tables and table structures
- `MiniDB`: Core class, implements various database operations
- `Column`: Column definition structure, stores column names and data types

## Development Environment

- Programming Language: C++
- Compiler: g++
- Operating System: Windows