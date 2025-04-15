## Question-3

Now on running the hiveql, we set Hive properties for dynamic bucketing:

1. SET hive.enforce.bucketing=true;
2. SET hive.enforce.sorting=true;

![Alt text](images/bucketing.png)


Now, the schema for the optimised dimensional and fact tables using data modelling concepts, and the effective concept of how the partitioning and bucketing will increase query performance.
The schema for the tables are as follows:-
```
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
```

You can see that,we have partitioned and bucketed it into optimised tables.  
The justification for that is as follows:-  

1. **`dim_grade_roster_optimised`**
- **Partitioned by `section`:**
  - Improves query performance for section-specific queries.
  - Ideal for filtering when analyzing grades per class or section.
- **Clustered by `student_id`:**
  - Boosts performance for student-wise joins (e.g., with attendance or fact table).
  - Enables efficient aggregation operations like CGPA computation.

2. **`dim_attendance_data_optimised`**
- **Partitioned by `course`:**
  - Optimizes queries analyzing attendance by course.
  - Reduces scan overhead for course-level reports.
- **Clustered by `member_id`:**
  - Enhances performance for per-student attendance analysis.
  - Useful in joins and aggregations involving individual students.

3. **`fact_table_optimised`**
- **No Partitioning:**
  - Acts as a central fact table joined with all dimensions.
  - Uniform query access across multiple attributes, so partitioning might not help.
- **Clustered by `member_id`:**
  - Speeds up joins with `dim_grade_roster_optimised` and `dim_attendance_data_optimised`.
  - Supports student-wise performance analysis (e.g., attendance + credit aggregation).


4. **`dim_enrollment_data_optimised`**
- **Partitioned by `subject_code_name`:**
  - Reduces data scanned for subject-specific queries or filters.
  - Common in analytics related to specific courses.
- **Clustered by `student_id`:**
  - Improves query performance for student-based tracking.
  - Useful for joins with grade and fact tables on `student_id`.


### Query-1

**Objective:**  
To compute the CGPA (Cumulative Grade Point Average) for each student based on the grade obtained and course credits.

**Approach:**  
- Join `dim_grade_roster_optimised` and `fact_table_optimised` on `student_id` and `subject_code_name`.
- Use a weighted sum of grade points (based on institutional grading system) multiplied by `course_credit`.
- Divide total weighted grade points by total credits to derive CGPA.
- Order results by CGPA and then by total credits in descending order.
- Since, we have done effective clustering and partitioning, we get the results in much lesser time, greater query performance.


**Use Case:**  
This query is essential for academic performance analysis, ranking students, and eligibility for honors or scholarships.

![Alt text](images/query-1%20bucketing.png)

### Query-2
**Objective:**  
To determine the number of students taught, average attendance, and maximum course credit for each faculty.

**Approach:**  
- Join `dim_grade_roster_optimised` and `fact_table_optimised` on student and course.
- Filter for only those students who have passed (`exam_result = 'Pass'`).
- Aggregate data to:
  - Count distinct students per faculty.
  - Calculate average attendance using `average_attendance_percent`.
  - Determine the highest credit course taught by each faculty.
- Since, we have done effective clustering and partitioning, we get the results in much lesser time, greater query performance.

**Use Case:**  
This helps analyze faculty engagement, workload distribution, and effectiveness in teaching based on student attendance and course difficulty.

![Alt text](images/query-2%20bucketing.png)

### Query-3

**Objective:**  
To identify students who have an attendance percentage below 75% in any course.

**Approach:**  
- Join `dim_grade_roster_optimised` and `fact_table_optimised` on `student_id` and `subject_code_name`.
- Calculate overall attendance percentage as (classes_attended / (attended + absent)) * 100:
- Filter (`HAVING`) to return only those records with less than 75% attendance.
- Since, we have done effective clustering and partitioning, we get the results in much lesser time, greater query performance.

**Use Case:**  
Used for academic warnings, eligibility checks for exams, and enforcing minimum attendance policies.


![Alt text](images/query-3%20bucketing.png)