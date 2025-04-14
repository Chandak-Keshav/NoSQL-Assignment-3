## Question-2

Before starting off, since we are utilising hive as a docker image due to various issues in the instllation as faced by many others, we are storing the tables everytime in our local system.
So, first we load csv of dimensional tables and fact table onto the docker image:
docker cp attendance.csv hive4:/tmp/dim_attendance.csv   
docker cp enrollment.csv hive4:/tmp/dim_enrollment.csv
docker cp grade.csv hive4:/tmp/dim_grade.csv
docker cp fact_table_final.csv hive4:/tmp/fact_table.csv
![Alt text](docker-cp.png)

Now on running the hiveql, we set Hive properties for dynamic bucketing:

1. SET hive.enforce.bucketing=true;
2. SET hive.enforce.sorting=true;

![Alt text](bucketing.png)

