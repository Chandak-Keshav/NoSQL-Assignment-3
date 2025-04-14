-- Join dim_grade_roster and fact_table
joined_data = JOIN grade_roster BY (student_id, subject_code_name), fact_table BY (member_id, course);

-- Calculate weighted points based on grades using CASE expression
cgpa_data = FOREACH joined_data GENERATE
    grade_roster::student_id AS student_id,
    grade_roster::course_credit AS course_credit,
    (CASE grade_roster::obtained_marks_grade
        WHEN 'A' THEN 4.0 * grade_roster::course_credit
        WHEN 'A-' THEN 3.7 * grade_roster::course_credit
        WHEN 'B+' THEN 3.4 * grade_roster::course_credit
        WHEN 'B' THEN 3.0 * grade_roster::course_credit
        WHEN 'B-' THEN 2.7 * grade_roster::course_credit
        WHEN 'C+' THEN 2.4 * grade_roster::course_credit
        WHEN 'C' THEN 2.0 * grade_roster::course_credit
        WHEN 'D' THEN 1.7 * grade_roster::course_credit
        ELSE 0.0
    END) AS weighted_points;

-- Group by student_id
grouped_data = GROUP cgpa_data BY student_id;

-- Calculate total credits and CGPA
result = FOREACH grouped_data {
    total_credits = SUM(cgpa_data.course_credit);
    total_weighted_points = SUM(cgpa_data.weighted_points);
    cgpa = total_weighted_points / total_credits;
    GENERATE group AS student_id, total_credits AS total_credits_completed, cgpa AS cgpa;
}

-- Order by CGPA and total credits descending
ordered_result = ORDER result BY cgpa DESC, total_credits_completed DESC;

-- Output the result
DUMP ordered_result;

STORE ordered_result INTO '/output/Query-1' USING PigStorage(',');