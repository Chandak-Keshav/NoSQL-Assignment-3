-- Join fact_table and dim_grade_roster
joined_data = JOIN fact_table BY (member_id, course), grade_roster BY (student_id, subject_code_name);

-- Group by student_id and subject_code_name
grouped_data = GROUP joined_data BY (grade_roster::student_id, grade_roster::subject_code_name);

-- Calculate attendance metrics
attendance_data = FOREACH grouped_data {
    total_attended = SUM(joined_data.fact_table::number_of_classes_attended);
    total_absent = SUM(joined_data.fact_table::number_of_classes_absent);
    attendance_percentage = (total_attended * 100.0) / (total_attended + total_absent);
    GENERATE FLATTEN(group) AS (student_id, course),
             total_attended AS total_classes_attended,
             total_absent AS total_classes_absent,
             attendance_percentage AS overall_attendance_percentage;
}

-- Filter for attendance below 75%
filtered_attendance = FILTER attendance_data BY overall_attendance_percentage < 75;

-- Output the result
DUMP filtered_attendance;