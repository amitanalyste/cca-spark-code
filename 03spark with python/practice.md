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

# read some text file
sc.textFile("sqoop_import/departments").take(1)

# looping into a rdd
data=sc.textFile("sqoop_import/departments")
for i in data.collect():
    print(i)

# reading from local fs
sc.textFile("file:///tmp/departments.json").take(3)

# using split to tokenize
>>> str = "hello, a word, after another one"
>>> str.split(",")
['hello', ' a word', ' after another one']
>>> str.split(",")[1:]
[' a word', ' after another one']

# using map
>>> rdd.map(lambda x : (None, x)).take(2)
[(None, u'{"dep_id":1 , "dep_name":"fitness"}'), (None, u'{"dep_id":2 , "dep_name":"footware"}')]


#using the map in the rdd

c.textFile("sqoop_import/departments")
>>> rdd.take(2)
[u'2,Fitness', u'3,Footwear']

# using the split to idx on the key and get the all the str as value
>>> rdd = sc.textFile("sqoop_import/order_items")
>>> for i in rdd.map(lambda x: tuple(x.split(","))).take(3):    print(i)
...
(u'1', u'1', u'957', u'1', u'299.98', u'299.98')
(u'2', u'2', u'1073', u'1', u'199.99', u'199.99')
(u'3', u'2', u'502', u'5', u'250.0', u'50.0')
>>> for i in rdd.map(lambda x: tuple(x.split(",",1))).take(3):  print(i)
...
(u'1', u'1,957,1,299.98,299.98')
(u'2', u'2,1073,1,199.99,199.99')
(u'3', u'2,502,5,250.0,50.0')
>>> for i in rdd.map(lambda x: tuple(x.split(",",2))).take(3):  print(i)
...
(u'1', u'1', u'957,1,299.98,299.98')
(u'2', u'2', u'1073,1,199.99,199.99')
(u'3', u'2', u'502,5,250.0,50.0')

# rading from seuqnece files
>>> rdd = sc.sequenceFile("pyspark/sqoop_import/order_items")                  # specifying the types
>>> rdd = sc.sequenceFile("pyspark/sqoop_import/order_items", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")

# using .saveAsNewAPIHadoopFile
>>> rdd = sc.textFile("sqoop_import/orders")
>>> rdd.take(1)
[u'1,2013-07-25 00:00:00.0,11599,CLOSED']
>>> rdd.map(lambda x: tuple(x.split(",", 1))).saveAsNewAPIHadoopFile("pyspark/order_items/TT","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")
>>>
 







































# Word count using pyspark


## Joining disparate data sets using pys park

## Aggregating data sets using pyspark - totals


## Aggregating data sets using pyspark - by key


## Filtering data using pyspark


## Sorting and Ranking using pyspark - global


## Sorting and Ranking using pyspark - by key


