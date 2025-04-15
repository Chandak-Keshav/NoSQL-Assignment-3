CREATE TABLE IF NOT EXISTS dim_grade_roster_optimised (
    academy_location STRING,
    student_id STRING,
    student_status STRING,
    admission_id STRING,
    admission_status STRING,
    program_name STRING,
    batch STRING,
    period STRING,
    faculty_name STRING,
    course_credit INT,
    obtained_marks_grade STRING,
    out_of_marks_grade STRING,
    exam_result STRING,
    subject_code_name STRING
)
PARTITIONED BY (section STRING)
CLUSTERED BY (student_id) INTO 8 BUCKETS 
STORED AS ORC;



CREATE TABLE IF NOT EXISTS dim_attendance_data_optimised (
    instructor STRING,
    name STRING,
    member_id STRING,
    number_of_classes_attended INT,
    number_of_classes_absent INT,
    average_attendance_percent FLOAT
)
PARTITIONED BY (course STRING)
CLUSTERED BY (member_id) INTO 8 BUCKETS 
STORED AS ORC;



CREATE TABLE IF NOT EXISTS fact_table_optimised (
    member_id STRING,
    course STRING,
    number_of_classes_attended INT,
    number_of_classes_absent INT,
    course_credit INT,
    average_attendance_percent FLOAT
)
CLUSTERED BY (member_id) INTO 8 BUCKETS 
STORED AS ORC;


CREATE TABLE IF NOT EXISTS dim_enrollment_data_optimised (
  serial_no INT,
  course_type STRING,
  student_id STRING,
  program STRING,
  batch STRING,
  period STRING,
  enrollment_date STRING,
  primary_faculty STRING,
  section STRING
)
PARTITIONED BY (subject_code_name STRING)
CLUSTERED BY (student_id) INTO 8 BUCKETS 
STORED AS ORC;
