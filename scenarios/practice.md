# sqoop


## test connection
[ma186082@quickstart scenarios]$ sqoop eval --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --password cloudera --query "show tables"

## import table
[ma186082@quickstart scenarios]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --table categories --password cloudera -m 1 &>tmp &

## import in a specified dir
[ma186082@quickstart scenarios]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --table categories --password cloudera -m 1 --target-dir "scenarios/sqoop/categories" &>tmp &
>[ma186082@quickstart scenarios]$ hdfs dfs -ls -R "scenarios/sqoop"

## import in a warehouse folder
[ma186082@quickstart scenarios]$ sqoop import --connect jdbc:mysql://quickstart:3306/retail_db --username retail_dba --table categories --password cloudera -m 1 --warehouse-dir "scenarios/sqoop/warehouse" &>tmp &
>[ma186082@quickstart scenarios]$ hdfs dfs -ls -R "scenarios/sqoop"


