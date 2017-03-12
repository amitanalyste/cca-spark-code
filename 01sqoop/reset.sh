#remove hdfs
hdfs dfs -rm -skipTrash -R /user/ma186082/sqoop_import/*
hdfs dfs -rm -skipTrash -R /user/hive/warehouse/*

hive -e "drop table categories"
hive -e "drop table customers"
hive -e "drop table departments"
hive -e "drop table order_items"
hive -e "drop table orders"
hive -e "drop table products"

