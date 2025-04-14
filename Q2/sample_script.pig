-- Load the data from a file
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);

-- Filter the data to get people older than 30
filtered_data = FILTER data BY age > 30;

-- Group the data by city
grouped_data = GROUP filtered_data BY city;

-- Calculate the average age for each city
avg_age = FOREACH grouped_data GENERATE group AS city, AVG(filtered_data.age) AS avg_age;

-- Store the result into an output file
STORE avg_age INTO 'output' USING PigStorage(',');

-- Dump the result to the console for viewing
DUMP avg_age;
