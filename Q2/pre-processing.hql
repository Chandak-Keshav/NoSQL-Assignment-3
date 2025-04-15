CREATE TABLE IF NOT EXISTS a_data (
    course STRING,
    instructor STRING,
    name STRING,
    email_id STRING,
    member_id STRING,
    number_of_classes_attended INT,
    number_of_classes_absent INT,
    average_attendance_percent FLOAT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1"); 

-- docker cp ~/Documents/docs/GradeRosterReport_v4.csv hive4:/tmp/GradeRosterReport_v4.csv
-- docker cp ~/Documents/docs/Enrollment_Data_v7.csv hive4:/tmp/Enrollment_Data_v7.csv
-- docker cp ~/Documents/docs/Course_Attendance.csv hive4:/tmp/Course_Attendance.csv


LOAD DATA LOCAL INPATH '/tmp/Course_Attendance.csv' INTO TABLE attendance_data;
LOAD DATA LOCAL INPATH '/tmp/Enrollment_Data_v7.csv' INTO TABLE enrollment_data;
LOAD DATA LOCAL INPATH '/tmp/GradeRosterReport_v4.csv' INTO TABLE grade_roster;



CREATE TABLE student_data.cleaned_attendance_data AS
SELECT
  CASE
    WHEN course LIKE 'T1-24-25-%' THEN SUBSTRING(course, 11)
    ELSE course
  END AS course_cleaned,
  instructor,
  name,
  email_id,
  member_id,
  number_of_classes_attended,
  number_of_classes_absent,
  average_attendance_percent
FROM attendance_data;


INSERT OVERWRITE TABLE student_data.cleaned_attendance_data
SELECT
  CASE
    WHEN course LIKE '%/%' THEN
      CONCAT(RTRIM(SUBSTRING(course, 1, LOCATE('/', course) - 1)),
             '/',
             LTRIM(SUBSTRING(course, LOCATE('/', course) + 1)))
    ELSE course
  END AS course_cleaned,
  instructor,
  name,
  email_id,
  member_id,
  number_of_classes_attended,
  number_of_classes_absent,
  average_attendance_percent
FROM attendance_data;


INSERT OVERWRITE TABLE student_data.cleaned_attendance_data
SELECT
  CASE
    WHEN course LIKE '% by %' THEN
      SUBSTRING(course, 1, LOCATE(' by ', course) - 1)
    ELSE course
  END AS course_cleaned,
  instructor,
  name,
  email_id,
  member_id,
  number_of_classes_attended,
  number_of_classes_absent,
  average_attendance_percent
FROM attendance_data;


INSERT OVERWRITE TABLE student_data.cleaned_attendance_data
SELECT
    CASE
        WHEN course_cleaned = 'AIM 511 Machine Learning' THEN 'AIM 511/Machine Learning'
        WHEN course_cleaned = 'DHS 304 User Research' THEN 'DHS 304/User Research'
        WHEN course_cleaned = 'DHS 301 Data Analysis and Visualization' THEN 'DHS 301/Data Analysis and Visualization'
        WHEN course_cleaned = 'GNL 801 Optimization' THEN 'GNL 801/Optimization'
        WHEN course_cleaned = 'NWC 882/Special Topics / Network-Based Computing for HPC' THEN 'NWC 882/Special Topics - Network-Based Computing for HPC'
        WHEN course_cleaned = 'EGC 103/Introdution to Computer Architecture and Operating Systems' THEN 'EGC 103/Introduction to Computer Architecture and Operating Systems'
        WHEN course_cleaned = 'AI 835 Self-Supervised Learning' THEN 'AI 835/Self-Supervised Learning'
        WHEN course_cleaned = 'DT 311 Software Product Management' THEN 'DT 311/Software Product Management'
        WHEN course_cleaned = 'EGC 223 /Computer Architecture - Memory' THEN 'EGC 223/Computer Architecture - Memory'
        ELSE course_cleaned
    END AS course_cleaned,
    instructor,
    name,
    email_id,
    member_id,
    number_of_classes_attended,
    number_of_classes_absent,
    average_attendance_percent
FROM student_data.cleaned_attendance_data;


CREATE TABLE student_data.cleaned_grade_data AS
SELECT
    CASE
        WHEN `subject_code_name` = 'AIM 511 Machine Learning' THEN 'AIM 511/Machine Learning'
        WHEN `subject_code_name` = 'DHS 304 User Research' THEN 'DHS 304/User Research'
        WHEN `subject_code_name` = 'DHS 301 Data Analysis and Visualization' THEN 'DHS 301/Data Analysis and Visualization'
        WHEN `subject_code_name` = 'GNL 801 Optimization' THEN 'GNL 801/Optimization'
        WHEN `subject_code_name` = 'NWC 882/Special Topics / Network-Based Computing for HPC' THEN 'NWC 882/Special Topics - Network-Based Computing for HPC'
        WHEN `subject_code_name` = 'EGC 103/Introdution to Computer Architecture and Operating Systems' THEN 'EGC 103/Introduction to Computer Architecture and Operating Systems'
        WHEN `subject_code_name` = 'AI 835 Self-Supervised Learning' THEN 'AI 835/Self-Supervised Learning'
        WHEN `subject_code_name` = 'DT 311 Software Product Management' THEN 'DT 311/Software Product Management'
        WHEN `subject_code_name` = 'EGC 223 /Computer Architecture - Memory' THEN 'EGC 223/Computer Architecture - Memory'
        ELSE `subject_code_name`
    END AS subject_code_cleaned,
    `student_id`,
    `student_name`,
    `program_name`,
    batch,
    period,
    `section`,
    `faculty_name`,
    `course_credit`,
    obtained_marks_grade AS grade,
    exam_result
FROM grade_roster;


CREATE TABLE student_data.cleaned_enrollment_data AS
SELECT
    CASE
        WHEN `subject_code_name` = 'AIM 511 Machine Learning' THEN 'AIM 511/Machine Learning'
        WHEN `subject_code_name` = 'DHS 304 User Research' THEN 'DHS 304/User Research'
        WHEN `subject_code_name` = 'DHS 301 Data Analysis and Visualization' THEN 'DHS 301/Data Analysis and Visualization'
        WHEN `subject_code_name` = 'GNL 801 Optimization' THEN 'GNL 801/Optimization'
        WHEN `subject_code_name` = 'NWC 882/Special Topics / Network-Based Computing for HPC' THEN 'NWC 882/Special Topics - Network-Based Computing for HPC'
        WHEN `subject_code_name` = 'EGC 103/Introdution to Computer Architecture and Operating Systems' THEN 'EGC 103/Introduction to Computer Architecture and Operating Systems'
        WHEN `subject_code_name` = 'AI 835 Self-Supervised Learning' THEN 'AI 835/Self-Supervised Learning'
        WHEN `subject_code_name` = 'DT 311 Software Product Management' THEN 'DT 311/Software Product Management'
        WHEN `subject_code_name` = 'EGC 223 /Computer Architecture - Memory' THEN 'EGC 223/Computer Architecture - Memory'
        ELSE `subject_code_name`
    END AS subject_code_cleaned,
    `student_id`,
    `student_name`,
    `program`,
    batch,
    period,
    `enrollment_date`,
    `primary_faculty`,
    `section`
FROM enrollment_data;


-- Export cleaned_attendance_data
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_exports/cleaned_attendance'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * FROM student_data.cleaned_attendance_data;

-- Export cleaned_enrollment_data
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_exports/cleaned_enrollment'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * FROM student_data.cleaned_enrollment_data;

-- Export cleaned_grade_data
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_exports/cleaned_grade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * FROM student_data.cleaned_grade_data;

-- Export fact_table
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/hive_exports/fact_table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * FROM student_data.fact_table;

-- docker cp hive4:/tmp/hive_exports/cleaned_attendance/000000_0 ./dim_attendance.csv
-- docker cp hive4:/tmp/hive_exports/cleaned_enrollment/000000_0 ./dim_enrollment.csv
-- docker cp hive4:/tmp/hive_exports/cleaned_grade/000000_0 ./dim_grade.csv
-- docker cp hive4:/tmp/hive_exports/fact_table/000000_0 ./fact_table.csv

INSERT INTO TABLE error_log
SELECT *
FROM a_data
WHERE (course NOT REGEXP '[0-9]' OR email_id = 'vishnu.raj@iiitb.org');

INSERT OVERWRITE DIRECTORY '/tmp/error_log_csv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM error_log;

-- docker cp hive4:/tmp/error_log_csv/000000_0 ./error_log.csv




