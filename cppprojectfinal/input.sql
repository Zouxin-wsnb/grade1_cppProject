CREATE DATABASE db_university;
USE DATABASE db_university;
CREATE TABLE student (
ID INTEGER,
Name TEXT,
GPA FLOAT,
Major TEXT
);
INSERT INTO student VALUES (1000, 'Jay Chou', 3.0, 'Microelectronics');
INSERT INTO student VALUES (1001, 'Taylor Swift', 3.2, 'Data Science');
INSERT INTO student VALUES (1002, 'Bob Dylan', 3.5, 'Financial Technology');
INSERT INTO student VALUES (1003, 'David Green', 3.7, 'Civil Engineering');
INSERT INTO student VALUES (1004, 'Hatsune Miku', 3.3, 'Vocaloid');
INSERT INTO student VALUES (1005, 'litterzy', 2.0, 'Cakewalk Producer');
CREATE TABLE enrollment (
StudentID INTEGER,
Course TEXT
);
INSERT INTO enrollment VALUES (1000, 'Microelectronics');
INSERT INTO enrollment VALUES (1001, 'Data Science');
INSERT INTO enrollment VALUES (1001, 'Machine Learning');
INSERT INTO enrollment VALUES (1002, 'Financial technology');
INSERT INTO enrollment VALUES (1003, 'Mapping Engineering');
INSERT INTO enrollment VALUES (1004, 'Unknown Mother Goose');
INSERT INTO enrollment VALUES (1004, 'Melt');
INSERT INTO enrollment VALUES (1004, 'ODDS&ENDS');
CREATE TABLE healthData (
Name TEXT,
height FLOAT,
weight FLOAT
);
INSERT INTO healthData VALUES ('Hatsune Miku', 158, 42.0);
INSERT INTO healthData VALUES ('litterzy', 178, 61.5);
UPDATE student
SET GPA = (GPA - 0.1) * 1 * (GPA - 1.5);
UPDATE student
SET GPA = (GPA -0.1) * 0.95
WHERE Name = 'Bob Dylan';
UPDATE healthData
SET height = 180.6, weight = 65
WHERE Name = 'litterzy';
SELECT * FROM student;
SELECT * FROM enrollment;
SELECT * FROM healthData;
SELECT student.Major, healthData.height
FROM student
INNER JOIN healthData
ON student.Name = healthData.Name;
SELECT student.Major, healthData.height
FROM student
INNER JOIN healthData
ON student.Name = healthData.Name
WHERE student.Name = 'Hatsune Miku';
DROP TABLE healthData;
SELECT student.Name, enrollment.Course
FROM student
INNER JOIN enrollment
ON student.ID = enrollment.StudentID;
SELECT ID, Name, GPA FROM student WHERE GPA > 3.3 OR Major = 'Data Science';
SELECT Name FROM student WHERE GPA < 3.1 OR Major = 'Financial Technology';
DELETE FROM student
WHERE GPA < 2.0;
SELECT * FROM student;
