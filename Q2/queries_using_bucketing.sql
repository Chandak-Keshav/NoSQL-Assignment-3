-- Query 1: Calculate CGPA for each student along with his total credits completed. The grading is utilised as per our institute
SELECT
  g.student_id,
  SUM(g.course_credit) AS total_credits_completed,
  SUM(CASE
        WHEN g.obtained_marks_grade = 'A'  THEN 4.0 * g.course_credit
        WHEN g.obtained_marks_grade = 'A-' THEN 3.7 * g.course_credit
        WHEN g.obtained_marks_grade = 'B+' THEN 3.4 * g.course_credit
        WHEN g.obtained_marks_grade = 'B'  THEN 3.0 * g.course_credit
        WHEN g.obtained_marks_grade = 'B-' THEN 2.7 * g.course_credit
        WHEN g.obtained_marks_grade = 'C+' THEN 2.4 * g.course_credit
        WHEN g.obtained_marks_grade = 'C'  THEN 2.0 * g.course_credit
        WHEN g.obtained_marks_grade = 'D'  THEN 1.7 * g.course_credit
        ELSE 0.0
      END) / SUM(g.course_credit) AS cgpa
FROM dim_grade_roster_optimised g
JOIN fact_table_optimised f
  ON g.student_id = f.member_id
  AND g.subject_code_name = f.course
WHERE g.obtained_marks_grade IN ('A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'D','S','P','F')
GROUP BY g.student_id
ORDER BY cgpa DESC, total_credits_completed DESC;

-- Query-2: Give the number of students, average attendance and maximum course credit for each faculty
SELECT
  g.faculty_name,
  COUNT(DISTINCT g.student_id) AS num_students, 
  AVG(f.average_attendance_percent) AS avg_attendance,
  MAX(g.course_credit) AS max_course_credit
FROM fact_table_optimised f
JOIN dim_grade_roster_optimised g
  ON f.member_id = g.student_id
  AND f.course = g.subject_code_name
WHERE g.exam_result = 'Pass'
GROUP BY g.faculty_name;

-- Query-3: Give the attendance percentage of each student in each course and then filter out those students and courses where attendance percentage is less than 75%
SELECT 
    g.student_id,
    g.subject_code_name AS course,
    SUM(f.number_of_classes_attended) AS total_classes_attended,
    SUM(f.number_of_classes_absent)  AS total_classes_absent,
    (SUM(f.number_of_classes_attended) * 100.0) / (SUM(f.number_of_classes_attended) + SUM(f.number_of_classes_absent)) AS overall_attendance_percentage
FROM fact_table_optimised f
INNER JOIN dim_grade_roster_optimised g
  ON f.member_id = g.student_id 
  AND f.course = g.subject_code_name
GROUP BY 
    g.student_id, 
    g.subject_code_name
HAVING 
    (SUM(f.number_of_classes_attended) * 100.0) / (SUM(f.number_of_classes_attended) + SUM(f.number_of_classes_absent)) < 75;