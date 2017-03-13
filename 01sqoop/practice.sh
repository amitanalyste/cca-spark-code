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

#def databse
hive -e "CREATE DATABASE IF NOT EXISTS retail_stage" 
sqoop import --options-file sqoop.opt --table orders --hive-import --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --hive-database retail_stage

#as parquet
sqoop import --options-file sqoop.opt --table orders -as-parquetfile --warehouse-dir /user/ma186082/sqoop_import

#avro 
sqoop import --options-file sqoop.opt --table orders -as-avrodatafile --target-dir /user/ma186082/sqoop_import/orders_avro

#boundary query
sqoop import --options-file sqoop.opt --table orders --boundary-query "select 1, 100, 1000 from orders limit 1" -m 3


