# practice with sqoop
### obar1

# using optionss-file 
sqoop import --options-file sqoop.opt --table departments --target-dir 001  &>tmp.log

# columsn
sqoop import --options-file sqoop.opt --table orders --target-dir 002  --columns  order_id,order_date  &>tmp.log

#where on fields
sqoop import --options-file sqoop.opt --table orders --target-dir 003 where "order_id > 100"  &>tmp.log

# free query and split by
sqoop import --options-file sqoop.opt  --query "select  * from orders where \$CONDITIONS"  --target-dir 004 --split-by order.id   &>tmp.log

# adding my where conditions
sqoop import --options-file sqoop.opt   --query " select  * from orders where order_id>1000 and \$CONDITIONS"   --target-dir 005 --split-by order.id  &>tmp.log

#validate the copy
sqoop import --options-file sqoop.opt   --table orders   --target-dir 006 --validate 

#append same folder
sqoop import --options-file sqoop.opt --table departments --target-dir 001  --append

#import all
sqoop import-all-tables --options-file sqoop.opt --hive-import

#snappy codec
sqoop import --options-file sqoop.opt --table orders --hive-import --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec
hdfs dfs -du -s -h /user/hive/w*/or*

#overwrite data
sqoop import --options-file sqoop.opt --table orders --hive-import --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-overwrite

#def database
hive -e "CREATE DATABASE IF NOT EXISTS retail_db" 
sqoop import --options-file sqoop.opt --table orders --hive-import --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-database retail_db

#as parquet
sqoop import --options-file sqoop.opt --table orders -as-parquetfile --warehouse-dir /user/ma186082/sqoop_import

#avro 
sqoop import --options-file sqoop.opt --table orders -as-avrodatafile --target-dir /user/ma186082/sqoop_import/orders_avro

#boundary query
sqoop import --options-file sqoop.opt --table orders --boundary-query "select 1, 100, 1000 from orders limit 1" -m 3

# fields delimited and lines terminated by
hive -f departments.hql
sqoop import --options-file sqoop.opt --table departments --fields-terminated-by '|' --lines-terminated-by '\n' --hive-database retail_db --hive-import --hive-table departments

## manual append
# initial data
sqoop import --options-file sqoop.opt --table orders --hive-import -hive-table retail_db.orders -where "order_id< 1000"
# adding the data with append modality
# adding new data in append
sqoop import --options-file sqoop.opt --table orders --hive-import -hive-table retail_db.orders --check-column order_id --incremental append --last-value 1000
sqoop eval --options-file sqoop.opt --query "select max(order_id) from orders"
hive -e "select max(order_id) from retail_db.orders"

## export
#create a table holder to which export data from hive
sqoop eval --options-file sqoop.opt --query "create table orders_export as select * from retail_db.orders where 1=0"
sqoop export --options-file sqoop.opt --table orders_export --export-dir /user/hive/warehouse/retail_db.db/orders --batch

# export with upsert
#create a table_holder to which export data from hive

sqoop import --options-file sqoop.opt --table departments --hive-import --create-hive-table  --hive-database retail_db
       
sqoop eval --options-file sqoop.opt --query "create table retail_db.departments_export as select * from retail_db.departments where 1=0"

hdfs dfs -cat /user/hive/warehouse/r*/dep*/* | head  > departments_export.001
vi departments_export.001 
hdfs dfs -put departments_export.001  /user/hive/warehouse/retail_db.db/departments/

sqoop export --options-file sqoop.opt --table departments_export --export-dir /user/hive/warehouse/retail_db.db/departments --update-key department_id --update-mode allowinsert 

