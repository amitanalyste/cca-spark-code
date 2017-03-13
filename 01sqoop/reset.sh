#remove hdfs
hdfs dfs -rm -skipTrash -R /user/ma186082/*
hdfs dfs -rm -skipTrash -R /user/hive/warehouse/*

hive -e "drop database retail_stage cascade"
hive -e "create database retail_stage"

