## Transform, Stage, Store using Spark with Python
# in spark-shell
```
spark-shell master local
spark-shell master yarn
```

```
val i: Int = 0
val num =1
```
```
val str: String = new String("hello man")
val abc = "abc"
println(str)
println(str+abc)
```

# copy hive-site 
`sudo ln -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml`

# run some sql on hive
`scala> sqlContext.sql("show tables").collect().foreach(println)`

[categories,false]
[customers,false]
[departments,false]

# retrieve data from hive and apply aggregation in spark

`scala> sqlContext.sql("select * from departments").count()`
res7: Long = 6

## Pyspark - Getting Started
# local lode
pyspal --master local
#yarn mode (deafult)
pyspark 
pyspark --master yarn


#using hive  via HiveContxt
```
from pyspark.sql import HiveContext
sample = sqlContext.sql("select * from departments")

for rec in sample.collect():
  print(rec)
```

Row(department_id=2, department_name=u'Fitness')
Row(department_id=3, department_name=u'Footwear')
Row(department_id=4, department_name=u'Apparel')
Row(department_id=5, department_name=u'Golf')
Row(department_id=6, department_name=u'Outdoors')
Row(department_id=7, department_name=u'Fan Shop')

#accessing remote db using jdbc

# use of /usr/share/java/mysql-connector-java.jar
/usr/share/java/mysql-connector-java.jar

os.environ['SPARK_CLASSPATH']="/usr/share/java/mysql-connector-java.jar"
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
jdbcurl="jdbc:mysql://quickstart.cloudera:3306/retail_db?user=root&password=cloudera"
df = sqlContext.load(source="jdbc", url=jdbcurl, dbtable="departments")
for rec  in df.collect():
	print(rec)


# using py scipt and submitting them
# code import_sqoop_data.py and create /user/ma186082/pyspark/ 
```
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("myPyspark")
sc = SparkContext(conf=conf)
dataRDD = sc.textFile("/user/ma186082/sqoop_import/departments")
for line  in dataRDD.collect():
	print(line)
dataRDD.saveAsTextFile("/user/ma186082/pyspark/departments")
```













## Move data between HDF-S and Spark - pyspark


## Word count using pyspark


## Joining disparate data sets using pys park


## Aggregating data sets using pyspark - totals


## Aggregating data sets using pyspark - by key


## Filtering data using pyspark


## Sorting and Ranking using pyspark - global


## Sorting and Ranking using pyspark - by key


