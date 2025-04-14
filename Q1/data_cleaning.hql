-- Step 1: Load data into Hive tables (assuming tables are already created)

-- Step 2: Remove rows with non-integer values in a specific column
CREATE TABLE filtered_enrollment_data AS
SELECT *
FROM enrollment_data
WHERE CAST(serial_no AS INT) IS NOT NULL;

-- Step 3: Filter by specific values in 'Course Type'
CREATE TABLE valid_course_types AS
SELECT *
FROM filtered_enrollment_data
WHERE course_type IN ('REGULAR', 'RE/PE/THESIS', 'RE/Thesis');

-- Step 4: Remove specific columns
CREATE TABLE enrollment_data_v2 AS
SELECT serial_no, course_type, student_id, student_name, program, batch, period, enrollment_date, primary_faculty, subject_code_name, section
FROM valid_course_types;

-- Step 5: Remove rows with NULL values
CREATE TABLE enrollment_data_v3 AS
SELECT *
FROM enrollment_data_v2
WHERE serial_no IS NOT NULL
  AND course_type IS NOT NULL
  AND student_id IS NOT NULL
  AND student_name IS NOT NULL
  AND program IS NOT NULL
  AND batch IS NOT NULL
  AND period IS NOT NULL
  AND enrollment_date IS NOT NULL
  AND primary_faculty IS NOT NULL
  AND subject_code_name IS NOT NULL
  AND section IS NOT NULL;

-- Step 6: Update column values (e.g., split and update strings)
CREATE TABLE enrollment_data_v4 AS
SELECT *,
  CASE
    WHEN subject_code_name LIKE '%/%' THEN SPLIT(subject_code_name, '/')[0]
    ELSE subject_code_name
  END AS updated_subject_code_name
FROM enrollment_data_v3;

-- Create a temporary table to hold the unique key and faculty name
CREATE TABLE temp_faculty_map AS
SELECT DISTINCT
  CONCAT(program_name, '-', batch, '-', period, '-', subject_code_name, '-', section) AS key,
  faculty_name
FROM graderosterreport
WHERE faculty_name IS NOT NULL;

-- Update the original table with missing faculty names
CREATE TABLE updated_graderosterreport AS
SELECT
  g.*,
  COALESCE(t.faculty_name, 'NA') AS updated_faculty_name
FROM graderosterreport g
LEFT JOIN temp_faculty_map t
ON CONCAT(g.program_name, '-', g.batch, '-', g.period, '-', g.subject_code_name, '-', g.section) = t.key;

-- Drop the temporary table
DROP TABLE IF EXISTS temp_faculty_map;

-- Create a new table without the unnecessary columns
CREATE TABLE cleaned_enrollment_data AS
SELECT
  course_type,
  student_id,
  student_name,
  program,
  batch,
  period,
  enrollment_date,
  primary_faculty,
  subject_code_name,
  section
FROM enrollment_data;

-- Update the Program Code/Name to Program Name
CREATE TABLE updated_program_name AS
SELECT
  *,
  SPLIT(program_code_name, '/')[1] AS program_name
FROM graderosterreport;

-- Update the Primary Faculty to have only one entry
CREATE TABLE updated_enrollment_data AS
SELECT
  *,
  SPLIT(primary_faculty, ',')[0] AS primary_faculty_single
FROM cleaned_enrollment_data;


-- Drop intermediate tables if needed
DROP TABLE IF EXISTS filtered_enrollment_data;
DROP TABLE IF EXISTS valid_course_types;
DROP TABLE IF EXISTS enrollment_data_v2;
DROP TABLE IF EXISTS enrollment_data_v3;