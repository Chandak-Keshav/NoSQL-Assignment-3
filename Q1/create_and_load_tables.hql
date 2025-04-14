-- Drop tables if they already exist (optional)
DROP TABLE IF EXISTS course_attendance;
DROP TABLE IF EXISTS enrollment_data;
DROP TABLE IF EXISTS grade_roster;

-- Create course_attendance table
CREATE TABLE course_attendance (
    course STRING,
    instructors ARRAY<STRING>,
    student_name STRING,
    email_id STRING,
    member_id STRING,
    classes_attended INT,
    classes_absent INT,
    average_attendance INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create enrollment_data table
CREATE TABLE enrollment_data (
    serial_no INT,
    course_type STRING,
    student_id STRING,
    student_name STRING,
    program STRING,
    batch STRING,
    period STRING,
    enrollment_date STRING,
    primary_faculty STRING,
    subject_code_name STRING,
    section STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Create grade_roster table
CREATE TABLE grade_roster (
    academy_location STRING,
    student_id STRING,
    student_status STRING,
    admission_id STRING,
    admission_status STRING,
    student_name STRING,
    program_name STRING,
    batch STRING,
    period STRING,
    subject_code_name STRING,
    section STRING,
    faculty_name STRING,
    course_credit INT,
    obtained_grade STRING,
    out_of_grade STRING,
    exam_result STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Load data into course_attendance table
LOAD DATA INPATH '/sunnykaushik/hive/warehouse/Course_Attendance.csv' INTO TABLE course_attendance;

-- Load data into enrollment_data table
LOAD DATA INPATH '/sunnykaushik/hive/warehouse/Enrollment_Data_v7.csv' INTO TABLE enrollment_data;

-- Load data into grade_roster table
LOAD DATA INPATH '/sunnykaushik/hive/warehouse/GradeRosterReport_v4.csv' INTO TABLE grade_roster;
