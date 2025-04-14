LOAD DATA LOCAL INPATH '/tmp/dim_attendance.csv' 
INTO TABLE dim_attendance_data;

LOAD DATA LOCAL INPATH '/tmp/dim_enrollment.csv' 
INTO TABLE dim_enrollment_data;

LOAD DATA LOCAL INPATH '/tmp/dim_grade.csv' 
INTO TABLE dim_grade_roster;

LOAD DATA LOCAL INPATH '/tmp/fact_table.csv'
INTO TABLE fact_table;
