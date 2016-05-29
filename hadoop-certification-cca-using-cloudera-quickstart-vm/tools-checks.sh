#!/bin/bash
sqoop version
hive --version

touch testing
hadoop fs -put testing /user/cloudera/
hadoop fs -ls /user/cloudera/*
rm testing
hadoop fs -rm   /user/cloudera/testing

spark-shell --version
impala-shell --version
flume ng version


#ps
ps -fu hdfs
ps -ef | grep -i manager
