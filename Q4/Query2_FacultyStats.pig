-- Join fact_table and dim_grade_roster
joined_data = JOIN fact_table BY (member_id, course), grade_roster BY (student_id, subject_code_name);

-- Filter for students who passed
filtered_data = FILTER joined_data BY grade_roster::exam_result == 'Pass';

-- Group by faculty_name
grouped_data = GROUP filtered_data BY grade_roster::faculty_name;

-- Calculate aggregations
result = FOREACH grouped_data {
    unique_students = DISTINCT filtered_data.grade_roster::student_id;
    GENERATE group AS faculty_name,
             COUNT(unique_students) AS num_students,
             AVG(filtered_data.fact_table::average_attendance_percent) AS avg_attendance,
             MAX(filtered_data.grade_roster::course_credit) AS max_course_credit;
}

-- Output the result
DUMP result;

STORE result INTO '/output/Query-2' USING PigStorage(',');