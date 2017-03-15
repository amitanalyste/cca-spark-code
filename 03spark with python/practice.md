# Transform, Stage, Store using Spark with Python

## in spark-shell
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

## copy hive-site 
`sudo ln -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml`

## run some sql on hive
`scala> sqlContext.sql("show tables").collect().foreach(println)`

[categories,false]
[customers,false]
[departments,false]

## retrieve data from hive and apply aggregation in spark

scala> sqlContext.sql("select * from departments").count()
res7: Long = 6

## Pyspark - Getting Started

## local mode
pyspal --master local
##yarn mode (deafult)
pyspark 
pyspark --master yarn


##using hivevia HiveContxt
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

##accessing remote db using jdbc

## use of /usr/share/java/mysql-connector-java.jar
/usr/share/java/mysql-connector-java.jar

os.environ['SPARK_CLASSPATH']="/usr/share/java/mysql-connector-java.jar"
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
jdbcurl="jdbc:mysql://quickstart.cloudera:3306/retail_db?user=root&password=cloudera"
df = sqlContext.load(source="jdbc", url=jdbcurl, dbtable="departments")
for recin df.collect():
	print(rec)


## using py scipt and submitting them
## code import_sqoop_data.py and create /user/ma186082/pyspark/ 
```
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("myPyspark")
sc = SparkContext(conf=conf)
dataRDD = sc.textFile("/user/ma186082/sqoop_import/departments")
for linein dataRDD.collect():
	print(line)
dataRDD.saveAsTextFile("/user/ma186082/pyspark/departments")
```

# Move data between HDF-S and Spark - pyspark

## read some text file
sc.textFile("sqoop_import/departments").take(1)

## looping into a rdd
data=sc.textFile("sqoop_import/departments")
for i in data.collect():
  print(i)

## reading from local fs
sc.textFile("file:///tmp/departments.json").take(3)

## using split to tokenize
str = "hello, a word, after another one"
str.split(",")
['hello', ' a word', ' after another one']
str.split(",")[1:]
[' a word', ' after another one']

## using map
rdd.map(lambda x : (None, x)).take(2)
[(None, u'{"dep_id":1 , "dep_name":"fitness"}'), (None, u'{"dep_id":2 , "dep_name":"footware"}')]


##using the map in the rdd

c.textFile("sqoop_import/departments")
rdd.take(2)
[u'2,Fitness', u'3,Footwear']

## using the split to idx on the key and get the all the str as value
rdd = sc.textFile("sqoop_import/order_items")
for i in rdd.map(lambda x: tuple(x.split(","))).take(3):  print(i)
...
(u'1', u'1', u'957', u'1', u'299.98', u'299.98')
(u'2', u'2', u'1073', u'1', u'199.99', u'199.99')
(u'3', u'2', u'502', u'5', u'250.0', u'50.0')
for i in rdd.map(lambda x: tuple(x.split(",",1))).take(3):  print(i)
...
(u'1', u'1,957,1,299.98,299.98')
(u'2', u'2,1073,1,199.99,199.99')
(u'3', u'2,502,5,250.0,50.0')
for i in rdd.map(lambda x: tuple(x.split(",",2))).take(3):  print(i)
...
(u'1', u'1', u'957,1,299.98,299.98')
(u'2', u'2', u'1073,1,199.99,199.99')
(u'3', u'2', u'502,5,250.0,50.0')

## reading from seuqnece files
rdd = sc.sequenceFile("pyspark/sqoop_import/order_items")                  

## specifying the types
rdd = sc.sequenceFile("pyspark/sqoop_import/order_items", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")

## using .saveAsNewAPIHadoopFile
rdd = sc.textFile("sqoop_import/orders")
rdd.take(1)
[u'1,2013-07-25 00:00:00.0,11599,CLOSED']
rdd.map(lambda x: tuple(x.split(",", 1))).saveAsNewAPIHadoopFile("pyspark/order_items/TT","org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")
 
 
## using he hivecontext in spark
from pyspark.sql import HiveContext
sqlc= HiveContext(sc)
data = sqlc.sql("select * from departments limit 1")
for i in data.collect():    print(i)
...

Row(order_id=1, order_date=u'2013-07-25 00:00:00.0
', order_customer_id=11599, order_status=u'CLOSED'
)

for i in data.collect():    print(i.order_id)

## using sql context with json, temp table creation

from pyspark import SQLContext
sqlc = SQLContext(sc)
departmentsJson = sqlc.jsonFile("departments.json")
departmentsJson.registerTempTable("tdepartments")
for i in sqlc.sql("select * from tdepartments").collect(): 	print(i)
...
Row(_corrupt_record=u'{department_id:2, department_name:Fitness}')
Row(_corrupt_record=u'{department_id:3, department_name:Footwear}')
Row(_corrupt_record=u'{department_id:4, department_name:Apparel}')
Row(_corrupt_record=u'{department_id:5, department_name:Golf}')
Row(_corrupt_record=u'{department_id:6, department_name:Outdoors}')
Row(_corrupt_record=u'{department_id:7, department_name:Fan Shop}')

## save as json
sqlc.sql("select * from tdepartments").toJSON().saveAsTextFile("tdepartments.json")
hdfs dfs -cat tdepartments.json



# Word count using pyspark


## wordcount on a text file
data =sc.textFile("wordcount.txt") 
dataFM = data.flatMap(lambda x: x.split(" "))
for i in dataFM.collect(): print(i)
...
MapReduce
is
a

dataM = data.map(lambda x: (x.split(" "),1))
for i in dataFM.collect(): print(i)
...
([u'MapReduce', u'is', u'a', u'programming', u'model', u'and', u'an', u'associated'], 1)
([u'implementation', u'for', u'processing', u'and', u'generating', u'large'], 1)
([u'data', u'sets.', u'Users', u'specify', u'a', u'map', u'function', u'that', u'processes', u'a'], 1)

## combine them together to implement the wordcount
dataFM = data.flatMap(lambda x: x.split(" "))
dataM = dataFM.map(lambda x: (x,1))
for i in dataM.take(3): print(i)
...
([u'MapReduce'], 1)
([u'is'], 1)
([u'a'], 1)

## reduce by key applied on the keys from the map
dataR = dataM.reduceByKey(lambda x,y: x + y)
dataR.take(3)
[(u'and', 7), (u'real', 1), (u'all', 1)]

dataR.saveAsTextFile("wc.out")



# Joining disparate data sets using pys park


# Aggregating data sets using pyspark - totals


# Aggregating data sets using pyspark - by key


# Filtering data using pyspark


# Sorting and Ranking using pyspark - global


# Sorting and Ranking using pyspark - by key


