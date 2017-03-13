CREATE TABLE retail_stage.departments (
department_id INT,
department_name STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;
