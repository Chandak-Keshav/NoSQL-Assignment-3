-- Load dim_enrollment_data
enrollment_data = LOAD '/path/to/dim_enrollment_data.csv' 
    USING PigStorage(',') 
    AS (
        serial_no:int,
        course_type:chararray,
        student_id:chararray,
        student_name:chararray,
        program:chararray,
        batch:chararray,
        period:chararray,
        enrollment_date:chararray,
        primary_faculty:chararray,
        subject_code_name:chararray,
        section:chararray
    );

-- Skip the header row
enrollment_data = FILTER enrollment_data BY serial_no IS NOT NULL AND serial_no != 'serial_no';

-- Load dim_grade_roster
grade_roster = LOAD '/path/to/dim_grade_roster.csv' 
    USING PigStorage(',') 
    AS (
        academy_location:chararray,
        student_id:chararray,
        student_status:chararray,
        admission_id:chararray,
        admission_status:chararray,
        student_name:chararray,
        program_name:chararray,
        batch:chararray,
        period:chararray,
        section:chararray,
        faculty_name:chararray,
        course_credit:int,
        obtained_marks_grade:chararray,
        out_of_marks_grade:chararray,
        exam_result:chararray,
        subject_code_name:chararray
    );

-- Skip the header row
grade_roster = FILTER grade_roster BY student_id IS NOT NULL AND student_id != 'student_id';

-- Load dim_attendance_data
attendance_data = LOAD '/path/to/dim_attendance_data.csv' 
    USING PigStorage(',') 
    AS (
        course:chararray,
        instructor:chararray,
        name:chararray,
        email_id:chararray,
        member_id:chararray,
        number_of_classes_attended:int,
        number_of_classes_absent:int,
        average_attendance_percent:float
    );

-- Skip the header row
attendance_data = FILTER attendance_data BY member_id IS NOT NULL AND member_id != 'member_id';

-- Load fact_table
fact_table = LOAD '/path/to/fact_table.csv' 
    USING PigStorage(',') 
    AS (
        member_id:chararray,
        course:chararray,
        number_of_classes_attended:int,
        number_of_classes_absent:int,
        course_credit:int,
        average_attendance_percent:float
    );

-- Skip the header row
fact_table = FILTER fact_table BY member_id IS NOT NULL AND member_id != 'member_id';

-- Optional: Store the relations to verify loading (uncomment to use)
-- STORE enrollment_data INTO '/path/to/output/enrollment_data' USING PigStorage(',');
-- STORE grade_roster INTO '/path/to/output/grade_roster' USING PigStorage(',');
-- STORE attendance_data INTO '/path/to/output/attendance_data' USING PigStorage(',');
-- STORE fact_table INTO '/path/to/output/fact_table' USING PigStorage(',');