-- Enable dynamic partitioning and bucketing enforcement
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Populate dim_grade_roster_optimised (partition by student_name)
INSERT OVERWRITE TABLE dim_grade_roster_optimised
PARTITION (student_name)
SELECT
    academy_location,
    student_id,
    student_status,
    admission_id,
    admission_status,
    program_name,
    batch,
    period,
    section,
    faculty_name,
    course_credit,
    obtained_marks_grade,
    out_of_marks_grade,
    exam_result,
    subject_code_name,
    student_name           -- This value fills the partition column
FROM dim_grade_roster;

-- Populate dim_attendance_data_optimised (partition by email_id)
INSERT OVERWRITE TABLE dim_attendance_data_optimised
PARTITION (email_id)
SELECT
    course,
    instructor,
    name,
    member_id,
    number_of_classes_attended,
    number_of_classes_absent,
    average_attendance_percent,
    email_id               -- This value fills the partition column
FROM dim_attendance_data;

-- Populate fact_table_optimised (non-partitioned, just bucketed)
INSERT OVERWRITE TABLE fact_table_optimised
SELECT
    member_id,
    course,
    number_of_classes_attended,
    number_of_classes_absent,
    course_credit,
    average_attendance_percent
FROM fact_table;

-- Populate dim_enrollment_data_optimised (partition by student_name)
INSERT OVERWRITE TABLE dim_enrollment_data_optimised
PARTITION (student_name)
SELECT
  serial_no,
  course_type,
  student_id,
  program,
  batch,
  period,
  enrollment_date,
  primary_faculty,
  subject_code_name,
  section,
  student_name         -- This value fills the partition column
FROM dim_enrollment_data;

-- Reset bucketing enforcement if needed
SET hive.enforce.bucketing = false;
