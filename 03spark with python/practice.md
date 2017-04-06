# Transform, Stage, Store using Spark with Python

## in spark-shell
```
spark-shell master local
spark-shell master yarn



val i: Int = 0
val num =1



val str: String = new String("hello man")
val abc = "abc"
println(str)
println(str+abc)

```
## copy hive-site
```
sudo ln -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml
```
## run some sql on hive
```
scala> sqlContext.sql("show tables").collect().foreach(println)

[categories,false]
[customers,false]
[departments,false]
```
## retrieve data from hive and apply aggregation in spark
```
scala> sqlContext.sql("select * from departments").count()
res7: Long = 6
```
## Pyspark - Getting Started
```
```
## local mode
```
pyspal --master local
```
## yarn mode (deafult)
```
pyspark
pyspark --master yarn

```
## using hivevia HiveContxt
```
from pyspark.sql import HiveContext
sample = sqlContext.sql("select * from departments")

for rec in sample.collect():
print(rec)

Row(department_id=2, department_name=u'Fitness')
Row(department_id=3, department_name=u'Footwear')
Row(department_id=4, department_name=u'Apparel')
Row(department_id=5, department_name=u'Golf')
Row(department_id=6, department_name=u'Outdoors')
Row(department_id=7, department_name=u'Fan Shop')
```
## accessing remote db using jdbc

## use of mysql connector /usr/share/java/mysql-connector-java.jar
```
os.environ['SPARK_CLASSPATH']="/usr/share/java/mysql-connector-java.jar"
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
jdbcurl="jdbc:mysql://quickstart.cloudera:3306/retail_db?user=root&password=cloudera"
df = sqlContext.load(source="jdbc", url=jdbcurl, dbtable="departments")
for recin df.collect():
	print(rec)

```
## using py script and submitting them
```
```
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
```
```
## read some text file
```
sc.textFile("sqoop_import/departments").take(1)
```
## looping into a rdd
```
data=sc.textFile("sqoop_import/departments")
for i in data.collect():
  print(i)

```
## reading from local fs
```
sc.textFile("file:///tmp/departments.json").take(3)

```
## using split to tokenize
```
str = "hello, a word, after another one"
str.split('\x01')
['hello', ' a word', ' after another one']
str.split('\x01')[1:]
[' a word', ' after another one']

```
## using map
```
rdd.map(lambda x : (None, x)).take(2)
[(None, u'{"dep_id":1 , "dep_name":"fitness"}'), (None, u'{"dep_id":2 , "dep_name":"footware"}')]


##using the map in the rdd

c.textFile("sqoop_import/departments")
rdd.take(2)
[u'2,Fitness', u'3,Footwear']

```
## using the split to idx on the key and get the all the str as value
```
rdd = sc.textFile("sqoop_import/order_items")
for i in rdd.map(lambda x: tuple(x.split('\x01'))).take(3):  print(i)
...
(u'1', u'1', u'957', u'1', u'299.98', u'299.98')
(u'2', u'2', u'1073', u'1', u'199.99', u'199.99')
(u'3', u'2', u'502', u'5', u'250.0', u'50.0')
for i in rdd.map(lambda x: tuple(x.split('\x01',1))).take(3):  print(i)
...
(u'1', u'1,957,1,299.98,299.98')
(u'2', u'2,1073,1,199.99,199.99')
(u'3', u'2,502,5,250.0,50.0')
for i in rdd.map(lambda x: tuple(x.split('\x01',2))).take(3):  print(i)
...
(u'1', u'1', u'957,1,299.98,299.98')
(u'2', u'2', u'1073,1,199.99,199.99')
(u'3', u'2', u'502,5,250.0,50.0')

```
## reading from sequence files
```
rdd = sc.sequenceFile("pyspark/sqoop_import/order_items")

```
## specifying the types
```
rdd = sc.sequenceFile("pyspark/sqoop_import/order_items", "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.Text")

```
## using .saveAsNewAPIHadoopFile
```
rdd = sc.textFile("sqoop_import/orders")
rdd.take(1)
[u'1,2013-07-25 00:00:00.0,11599,CLOSED']
rdd.map(lambda x: tuple(x.split('\x01', 1))).saveAsNewAPIHadoopFile("pyspark/order_items/TT'\x01'org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat",keyClass="org.apache.hadoop.io.Text",valueClass="org.apache.hadoop.io.Text")
 
```
## using he hivecontext in spark
```
from pyspark.sql import HiveContext
sqlc= HiveContext(sc)
data = sqlc.sql("select * from departments limit 1")
for i in data.collect():    print(i)
...

Row(order_id=1, order_date=u'2013-07-25 00:00:00.0
', order_customer_id=11599, order_status=u'CLOSED'
)

for i in data.collect():    print(i.order_id)


```
## using sql context with json, temp table creation
```

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

```
## save as json
```
sqlc.sql("select * from tdepartments").toJSON().saveAsTextFile("tdepartments.json")
hdfs dfs -cat tdepartments.json


```
# Word count using pyspark
```
```
## wordcount on a text file
```
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
([u```'data', u'sets.', u'Users', u'specify', u'a', u'map', u'function', u'that', u'processes', u'a'], 1)
```
## combine them together to implement the wordcount
```
dataFM = data.flatMap(lambda x: x.split(" "))
dataM = dataFM.map(lambda x: (x,1))
for i in dataM.take(3): print(i)
...
([u'MapReduce'], 1)
([u'is'], 1)
([u'a'], 1)

```
## reduce by key applied on the keys from the map
```
dataR = dataM.reduceByKey(lambda x,y: x + y)
dataR.take(3)
[(u'and', 7), (u'real', 1), (u'all', 1)]

dataR.saveAsTextFile("wc.out")


```
# Joining disparate data sets using pys park
```

> Problem statement, get the revenue and number of orders from order_items on daily basis
```
## get orders and order items 
```
ordersRDD = sc.textFile("sqoop_import/orders")
orderItemsRDD = sc.textFile("sqoop_import/order_items")

```
## check the contents
```
for i in ordersRDD.map(lambda o : (o.split('\x01') )).take(3) : print(i)
...
[u'1', u'2013-07-25 00:00:00.0', u'11599', u'CLOSED']
[u'2', u'2013-07-25 00:00:00.0', u'256', u'PENDING_PAYMENT']
[u'3', u'2013-07-25 00:00:00.0', u'12111', u'COMPLETE']

for i in ordersRDD.map(lambda o : (o.split('\x01')[0], o.split('\x01')[1] )).take(3) : print(i)
...```
(u'1', u'2013-07-25 00:00:00.0')
(u'2',``` u'2013-07-25 00:00:00.0')
(u'3', u'2013-07-25 00:00:00.0')

```
## only relevant fields for problem and proper type
```
for i in ordersRDD.map(lambda o : (int(o.split('\x01')[0]), o.split('\x01')[1] )).take(3) : print(i)
...
(1, u'2013-07-25 00:00:00.0')
(2, u'2013-07-25 00:00:00.0')
(3, u'2013-07-25 00:00:00.0')

```
## to join 2 tables on order_id we need pk order_id and record from orders and fk order_id and record from order_items
```
ordersParsedRDD = ordersRDD.map(lambda rec: (int(rec.split('\x01')[0]), rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (int(rec.split('\x01')[1]), rec))

```
## joined table
```
ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)

for rec in ordersJoinOrderItems.first():     print(rec)
...
2
(u'2,2,1073,1,199.99,199.99', u'2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT')

```
## drilling int the result
```
for rec in ordersJoinOrderItems.map(lambda o: (o[0])).take(3):       print(rec)                   ...
2
2
2
for rec in ordersJoinOrderItems.map(lambda o: (o[1])).take(3):       print(rec)
...
(u'2,2,1073,1,199.99,199.99', u'2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT')
(u'3,2,502,5,250.0,50.0', u'2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT')
(u'4,2,403,1,129.99,129.99', u'2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT')
for rec in ordersJoinOrderItems.map(lambda o: (o[1][0])).take(3):    print(rec)
...
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
for rec in ordersJoinOrderItems.map(lambda o: (o[1][1])).take(3):    print(rec)
...
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT

```
## get only relevant fields for the problem (sub_total and day)
```
revenuePerOrderPerDayRDD = ordersJoinOrderItems.map(lambda t: (t[1][1].split('\x01')[1], float(t[1][0].split('\x01')[4])))
 in revenuePerOrderPerDayRDD.take(3):  print(i)
...
(u'2013-07-25 00:00:00.0', 199.99000000000001)
(u'2013-07-25 00:00:00.0', 250.0)
(u'2013-07-25 00:00:00.0', 129.99000000000001)

```
## build items with 
```
for i in ordersJoinOrderItems.map(lambda rec: rec[1][1].split('\x01')[1] + "|" + str(rec[0])).take(10):        print(i)
...
2013-07-25 00:00:00.0|2
2013-07-25 00:00:00.0|2
2013-07-25 00:00:00.0|2
2013-07-25 00:00:00.0|4
2013-07-25 00:00:00.0|4
ordersPerDay= ordersJoinOrderItems.map(lambda rec: rec[1][1].split('\x01')[1] + "|" + str(rec[0]))

for i in ordersPerDay.map(lambda rec: (rec.split("|")[0], 1)).first(): print(i)
...
2013-07-25 00:00:00.0
1

ordersPerDayParsed = ordersPerDay.map(lambda rec: (rec.split("|")[0], 1))

```
## reduce to get the total order_item_subtotal
```
for i in ordersPerDayParsed.reduceByKey(lambda x, y: x + y).take(10): print (i)
...
(u'2013-09-19 00:00:00.0', 507)
(u'2013-11-29 00:00:00.0', 676)
(u'2013-10-28 00:00:00.0', 330)
(u'2013-08-21 00:00:00.0', 316)
(u'2013-12-06 00:00:00.0', 652)
(u'2014-01-21 00:00:00.0', 620)
(u'2013-08-10 00:00:00.0', 639)
(u'2014-06-17 00:00:00.0', 354)
(u'2013-11-07 00:00:00.0', 588)
(u'2014-03-25 00:00:00.0', 291)

subtotalOrdersPerDay = ordersPerDayParsed.reduceByKey(lambda x, y: x + y)
```
## get revenue per day from joined data
```python
totalRevenuePerDay = subtotalOrdersPerDay.reduceByKey( \
lambda total1, total2: total1 + total2 \
)

for data in totalRevenuePerDay.collect():
  print(data)

```
## checks in mysql
```sql
mysql> select order_date , order_item_subtotal from order_items oi inner join orders o on  oi.order_item_order_id = o.order_id  where order_date like '2013-07-25%' ;

```
## Joining order count per day and revenue per day
```python
finalJoinRDD = totalOrdersPerDay.join(totalRevenuePerDay)
for data in finalJoinRDD.take(5):
  print(data)

```
### Using Hive
```python
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

joinAggData = sqlContext.sql("select o.order_date, round(sum(oi.order_item_subtotal), 2), \
count(distinct o.order_id) from orders o join order_items oi \
on o.order_id = oi.order_item_order_id \
group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)

```
### Using spark native sql
```python
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
sqlContext.sql("set spark.sql.shuffle.partitions=10");

ordersRDD = sc.textFile("sqoop_import/orders")
ordersMap = ordersRDD.map(lambda o: o.split('\x01'))
orders = ordersMap.map(lambda o: Row(order_id=int(o[0]), order_date=o[1], \
order_customer_id=int(o[2]), order_status=o[3]))
ordersSchema = sqlContext.inferSchema(orders)
ordersSchema.registerTempTable("orders")

orderItemsRDD = sc.textFile("sqoop_import/order_items")
orderItemsMap = orderItemsRDD.map(lambda oi: oi.split('\x01'))
orderItems = orderItemsMap.map(lambda oi: Row(order_item_id=int(oi[0]), order_item_order_id=int(oi[1]), \
order_item_product_id=int(oi[2]), order_item_quantity=int(oi[3]), order_item_subtotal=float(oi[4]), \
order_item_product_price=float(oi[5])))
orderItemsSchema = sqlContext.inferSchema(orderItems)
orderItemsSchema.registerTempTable("order_items")

joinAggData = sqlContext.sql("select o.order_date, sum(oi.order_item_subtotal),count(distinct o.order_id) from orders o join order_items oi on o.order_id = oi.order_item_order_id  group by o.order_date order by o.order_date")

for data in joinAggData.collect():
  print(data)


```
# Aggregating data sets using pyspark - totals
```
```
## Get max priced product from products table
```python
orders = sc.textFile("sqoop_import/orders")
orderItems = sc.textFile("sqoop_import/order_items")
orderItemsMap = orderItems.map(lambda rec: float(rec.split("|")[4]))
for i in orderItemsMap.take(5): print i

orderItemsReduce = orderItemsMap.reduce(lambda rev1, rev2: rev1 + rev2)

``` 
### pyspark script to get the max priced product
```python
products = sc.textFile("sqoop_import/products")
productsMap = products.map(lambda rec: rec)
productsMap.reduce(lambda rec1, rec2: (rec1 if( float(rec1.split("|")[4]) >= float(rec2.split("|")[4])) else rec2))
```
### avg
```python
totalRevenue = sc.textFile("sqoop_import/order_items").map(lambda rec: float(rec.split('\x01')[4])).reduce(lambda acc, val: acc + rev2)
totalOrders = sc.textFile("sqoop_import/order_items").map(lambda rec: int(rec.split('\x01')[1])).distinct().count()
avg = totalRevenue / totalOrders

```
# Aggregating data sets using pyspark - by key
```
```
## counting order status	
```python
>>> orders.map(lambda rec: (rec.split("|")[3], 0 ))
>>> orders.map(lambda rec: (rec.split("|")[3], 0 )).countByKey()
defaultdict(<type 'int'>, {u'COMPLETE': 22899, u'PAYMENT_REVIEW': 729, u'PROCESSING': 8275, u'CANCELED': 1428, u'PENDING': 7610, u'CLOSED': 7556, u'PENDING_PAYMENT': 15030, u'SUSPECTED_FRAUD': 1558, u'ON_HOLD': 3798})
>>> orders.map(lambda rec: (rec.split("|")[3], 0 )).countByKey().items()
[(u'COMPLETE', 22899), (u'PAYMENT_REVIEW', 729), (u'PROCESSING', 8275), (u'CANCELED', 1428), (u'PENDING', 7610), (u'CLOSED', 7556), (u'PENDING_PAYMENT', 15030), (u'SUSPECTED_FRAUD', 1558), (u'ON_HOLD', 3798)]


orders = sc.textFile("sqoop_import/orders")

#groupbykey
ordersMap = orders.map(lambda rec: (rec.split("|")[3], 1) )
ordersByStatusGBK = ordersMap.groupByKey().map(lambda t: (t[0], sum(t[1])))
for i in ordersMap.countByKey().items(): print(i)

#reduce by key (using the accumulator)
ordersByStatusRBK = ordersMap.reduceByKey(lambda acc, val: acc+val)

#aggregate by key
ordersByStatusABK = ordersMap.aggregateByKey(0, lambda acc, value: acc+1, lambda acc, value: acc+value)

#combine by key
ordersByStatusCBK = ordersMap.combineByKey(lambda value: (1), lambda acc, value: (acc+1), lambda acc, value: (acc+value) )


```
## total Revenue per day
```python
ordersRDD = sc.textFile("sqoop_import/orders")
orderItemsRDD = sc.textFile("sqoop_import/order_items")

ordersParsedRDD = ordersRDD.map(lambda rec: (rec.split("|")[0], rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (rec.split("|")[1], rec))

ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
ordersJoinOrderItemsMap = ordersJoinOrderItems.map(lambda t: (t[1][1].split("|")[1], float(t[1][0].split("|")[4])))

revenuePerDay = ordersJoinOrderItemsMap.reduceByKey(lambda acc, value: acc + value)
for i in revenuePerDay.collect(): print(i)

```
## average revenue per day
```python
>Use appropriate aggregate function to get sum(order_item_subtotal) for each order_date, order_id combination

>Parse data to discard order_id and get order_date as key and sum(order_item_subtotal) per order as value

>Use appropriate aggregate function to get sum(order_item_subtotal) per day and count(distinct order_id) per day


ordersRDD = sc.textFile("sqoop_import/orders")
orderItemsRDD = sc.textFile("sqoop_import/order_items")

ordersParsedRDD = ordersRDD.map(lambda rec: (rec.split("|")[0], rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (rec.split("|")[1], rec))

ordersJoinOrderItems = orderItemsParsedRDD.join(ordersParsedRDD)
ordersJoinOrderItemsMap = ordersJoinOrderItems.map(lambda t: ((t[1][1].split("|")[1], t[0]), float(t[1][0].split("|")[4])))

>>> for i in ordersJoinOrderItemsMap.sortByKey().take(5): print(i)
((u'2013-07-25 00:00:00.0', u'1'), 299.98000000000002)
((u'2013-07-25 00:00:00.0', u'10'), 199.99000000000001)
((u'2013-07-25 00:00:00.0', u'10'), 99.959999999999994)
((u'2013-07-25 00:00:00.0', u'10'), 129.99000000000001)
((u'2013-07-25 00:00:00.0', u'10'), 21.989999999999998)


revenuePerDayPerOrder = ordersJoinOrderItemsMap.reduceByKey(lambda acc, value: acc + value)

>>> for i in revenuePerDayPerOrder.sortByKey().take(5): print(i)             ...
((u'2013-07-25 00:00:00.0', u'1'), 299.98000000000002)
((u'2013-07-25 00:00:00.0', u'10'), 651.92000000000007)
((u'2013-07-25 00:00:00.0', u'100'), 549.94000000000005)
((u'2013-07-25 00:00:00.0', u'101'), 899.94000000000005)
((u'2013-07-25 00:00:00.0', u'103'), 829.92000000000007)


revenuePerDayPerOrderMap = revenuePerDayPerOrder.map(lambda rec: (rec[0][0], rec[1]))

>>>revenuePerDayPerOrderMap.count()
57431
>>> for i in revenuePerDayPerOrderMap.sortByKey().take(5): print(i)          
(u'2013-07-25 00:00:00.0', 549.94000000000005)
(u'2013-07-25 00:00:00.0', 579.98000000000002)
(u'2013-07-25 00:00:00.0', 599.89999999999998)
(u'2013-07-25 00:00:00.0', 699.88999999999999)
(u'2013-07-25 00:00:00.0', 150.0)


revenuePerDay = revenuePerDayPerOrderMap.combineByKey( \
lambda x: (x, 1), \
lambda acc, revenue: (acc[0] + revenue, acc[1] + 1), \
lambda total1, total2: (round(total1[0] + total2[0], 2), total1[1] + total2[1]) \
)

>>> for i in revenuePerDay.sortByKey().take(5): print(i)
(u'2013-07-25 00:00:00.0', (68153.830000000002, 116))
(u'2013-07-26 00:00:00.0', (136520.17000000001, 233))
(u'2013-07-27 00:00:00.0', (101074.34, 175))
(u'2013-07-28 00:00:00.0', (87123.080000000002, 158))
(u'2013-07-29 00:00:00.0', (137287.09, 216))
>>> revenuePerDay.count()
364


avgRevenuePerDay = revenuePerDaymap(lambda x: (x[0], x[1][0] / x[1][1]))

>>> for i in avgRevenuePerDay.sortByKey().take(5): print(i)
...
(u'2013-07-25 00:00:00.0', 587.5330172413793)
(u'2013-07-26 00:00:00.0', 585.92347639484979)
(u'2013-07-27 00:00:00.0', 577.56765714285712)
(u'2013-07-28 00:00:00.0', 551.41189873417727)
(u'2013-07-29 00:00:00.0', 635.58837962962957)
>>> avgRevenuePerDay.count()
364

```
# Filtering data using pyspark
```
```
## into a smaller dataset using Spark
```python
ordersRDD = sc.textFile("/user/hive/warehouse/retail_db.db/orders")
for i in ordersRDD.filter(lambda line: line.split('\x01')[3] == "COMPLETE").take(5): print(i)

for i in ordersRDD.filter(lambda line: "PENDING" in line.split('\x01')[3]).take(5): print(i)

for i in ordersRDD.filter(lambda line: int(line.split('\x01')[0]) > 100).take(5): print(i)
 
for i in ordersRDD.filter(lambda line: int(line.split('\x01')[0]) > 100 or line.split('\x01')[3] in "PENDING").take(5): print(i)
 
for i in ordersRDD.filter(lambda line: int(line.split('\x01')[0]) > 1000 and ("PENDING" in line.split('\x01')[3] or line.split('\x01')[3] == ("CANCELLED"))).take(5): print(i)
 
for i in ordersRDD.filter(lambda line: int(line.split('\x01')[0]) > 1000 and line.split('\x01')[3] != ("COMPLETE")).take(5): print(i)

```
```
## Check if there are any cancelled orders with amount greater than 1000$
```python
#Get only cancelled orders
#Join orders and order items
#Generate sum(order_item_subtotal) per order
#Filter data which amount to greater than 1000$

ordersRDD = sc.textFile("/user/hive/warehouse/retail_db.db/orders")
orderItemsRDD = sc.textFile("/user/hive/warehouse/retail_db.db/order_items")

ordersParsedRDD = ordersRDD.filter(lambda rec: rec.split('\x01')[3] in "CANCELED").map(lambda rec: (int(rec.split('\x01')[0]), rec))
orderItemsParsedRDD = orderItemsRDD.map(lambda rec: (int(rec.split('\x01')[1]), float(rec.split('\x01')[4])))
orderItemsAgg = orderItemsParsedRDD.reduceByKey(lambda acc, value: (acc + value))

ordersJoinOrderItems = orderItemsAgg.join(ordersParsedRDD)

for i in ordersJoinOrderItems.filter(lambda rec: rec[1][0] >= 1000).take(5): print(i)

```
# Sorting and Ranking using pyspark - global
# Write a query that produces ranked or sorted data using Spark

## Global sorting and ranking
```python
orders = sc.textFile("/user/hive/warehouse/retail_db.db/orders")
>>> for i in orders.map(lambda rec: (int(rec.split('\x01')[0]), rec)).sortByKey().take(5): print(i)
...
(1, u'1\x012013-07-25 00:00:00.0\x0111599\x01CLOSED')
(2, u'2\x012013-07-25 00:00:00.0\x01256\x01PENDING_PAYMENT')
(3, u'3\x012013-07-25 00:00:00.0\x0112111\x01COMPLETE')
(4, u'4\x012013-07-25 00:00:00.0\x018827\x01CLOSED')
(5, u'5\x012013-07-25 00:00:00.0\x0111318\x01COMPLETE')
>>> for i in orders.map(lambda rec: (int(rec.split('\x01')[0]), rec)).sortByKey(False).take(5): print(i)
...
(68883, u'68883\x012014-07-23 00:00:00.0\x015533\x01COMPLETE')
(68882, u'68882\x012014-07-22 00:00:00.0\x0110000\x01ON_HOLD')
(68881, u'68881\x012014-07-19 00:00:00.0\x012518\x01PENDING_PAYMENT')
(68880, u'68880\x012014-07-13 00:00:00.0\x011117\x01COMPLETE')
(68879, u'68879\x012014-07-09 00:00:00.0\x01778\x01COMPLETE')
>>> for i in orders.map(lambda rec: (int(rec.split('\x01')[0]), rec)).top(5): print(i)
...
(68883, u'68883\x012014-07-23 00:00:00.0\x015533\x01COMPLETE')
(68882, u'68882\x012014-07-22 00:00:00.0\x0110000\x01ON_HOLD')
(68881, u'68881\x012014-07-19 00:00:00.0\x012518\x01PENDING_PAYMENT')
(68880, u'68880\x012014-07-13 00:00:00.0\x011117\x01COMPLETE')
(68879, u'68879\x012014-07-09 00:00:00.0\x01778\x01COMPLETE')
>>> for i in orders.map(lambda rec: (int(rec.split('\x01')[0]), rec)).takeOrdered(5, lambda x: x[0]): print(i)
...
(1, u'1\x012013-07-25 00:00:00.0\x0111599\x01CLOSED')
(2, u'2\x012013-07-25 00:00:00.0\x01256\x01PENDING_PAYMENT')
(3, u'3\x012013-07-25 00:00:00.0\x0112111\x01COMPLETE')
(4, u'4\x012013-07-25 00:00:00.0\x018827\x01CLOSED')
(5, u'5\x012013-07-25 00:00:00.0\x0111318\x01COMPLETE')
>>> for i in orders.map(lambda rec: (int(rec.split('\x01')[0]), rec)).takeOrdered(5, lambda x: -x[0]): print(i)
...
(68883, u'68883\x012014-07-23 00:00:00.0\x015533\x01COMPLETE')
(68882, u'68882\x012014-07-22 00:00:00.0\x0110000\x01ON_HOLD')
(68881, u'68881\x012014-07-19 00:00:00.0\x012518\x01PENDING_PAYMENT')
(68880, u'68880\x012014-07-13 00:00:00.0\x011117\x01COMPLETE')
(68879, u'68879\x012014-07-09 00:00:00.0\x01778\x01COMPLETE')
>>> for i in orders.takeOrdered(5, lambda x: int(x.split('\x01')[0])): print(i)
...
12013-07-25 00:00:00.011599CLOSED
22013-07-25 00:00:00.0256PENDING_PAYMENT
32013-07-25 00:00:00.012111COMPLETE
42013-07-25 00:00:00.08827CLOSED
52013-07-25 00:00:00.011318COMPLETE

```
# Sorting and Ranking using pyspark - by key

>>> l = [2,4,1,2,3,-1,5,10]
>>> len(l)
8
>>> sorted(l)
[-1, 1, 2, 2, 3, 4, 5, 10]
>>> set(l)
set([1, 2, 3, 4, 5, 10, -1])
>>> sorted(l, reverse=True)
[10, 5, 4, 3, 2, 2, 1, -1]
>>> import itertools
>>> itertools.islice(l, 0,2)
<itertools.islice object at 0x145ad08>
>>> for i in itertools.islice(l, 0,2): print(i)
...
2
4
>>> for i in range(0,2): print(l[i])
...
2
4
>>> list(itertools.islice(l, 0,2))
[2, 4]
>>> list(itertools.islice(sorted(l), 0,2))
[-1, 1]

## top n producs by category by price

products = sc.textFile("/user/hive/warehouse/retail_db.db/products")
productsMap = products.map(lambda rec: ( rec.split('\x01')[1], rec) )
for i in productsMap.groupByKey().take(3): print(i);
...
(u'11', <pyspark.resultiterable.ResultIterable object at 0x24e0190>)
(u'24', <pyspark.resultiterable.ResultIterable object at 0x24e03d0>)
(u'15', <pyspark.resultiterable.ResultIterable object at 0x24e0490>)
for i in productsMap.groupByKey().map(lambda rec : (rec[0], list(rec[1]))).take(3): print(i);
productsGroupBy = productsMap.groupByKey()
## sorted product gategory
>>> for i in productsGroupBy.flatMap(lambda rec: sorted(rec[1])).take(5):       print(i)
## sort by price now
>>> for i in productsGroupBy.flatMap(lambda rec: sorted(rec[1], key=lambda k: k.split('\x01'))).take(5):  print(i)
...
21711Fitness Gear 300 lb Olympic Weight Set209.99http://images.acmesports.sports/Fitness+Gear+300+lb+Olympic+Weight+Set
21811Elevation Training Mask 2.079.99http://images.acmesports.sports/Elevation+Training+Mask+2.0
21911Fitness Gear Pro Utility Bench179.99http://images.acmesports.sports/Fitness+Gear+Pro+Utility+Bench
22011Teeter Hang Ups NXT-S Inversion Table299.99http://images.acmesports.sports/Teeter+Hang+Ups+NXT-S+Inversion+Table
22111Fitness Gear Pro Olympic Bench249.99http://images.acmesports.sports/Fitness+Gear+Pro+Olympic+Bench
>>> for i in productsGroupBy.flatMap(lambda rec: sorted(rec[1], key=lambda k: float(k.split('\x01')[4]))).take(5):        print(i)
...
23511Under Armour Hustle Storm Medium Duffle Bag34.99http://images.acmesports.sports/Under+Armour+Hustle+Storm+Medium+Duffle+Bag
21811Elevation Training Mask 2.079.99http://images.acmesports.sports/Elevation+Training+Mask+2.0
23711Fitness Gear 7' Olympic Bar79.99http://images.acmesports.sports/Fitness+Gear+7%27+Olympic+Bar
23211adidas Men's Powerlift 2 Training Shoe89.99http://images.acmesports.sports/adidas+Men%27s+Powerlift+2+Training+Shoe
23811Fitness Gear Heavy Bag Stand99.99http://images.acmesports.sports/Fitness+Gear+Heavy+Bag+Stand
>>>
## add reverse
>>> for i in productsGroupBy.flatMap(lambda rec: sorted(rec[1], key=lambda k: float(k.split('\x01')[4]), reverse=True)).take(5):  print(i)
## get topN priced products by category
def getTopDenseN(rec, topN):
  x = [ ]
  topNPrices = [ ]
  prodPrices = [ ]
  prodPricesDesc = [ ]
  for i in rec[1]:
    prodPrices.append(float(i.split('\x01')[4]))
  prodPricesDesc = list(sorted(set(prodPrices), reverse=True))
  import itertools
  topNPrices = list(itertools.islice(prodPricesDesc, 0, topN))
  for j in sorted(rec[1], key=lambda k: float(k.split('\x01')[4]), reverse=True):
    if(float(j.split('\x01')[4]) in topNPrices):
      x.append(j)
  return (y for y in x)

for i in productsMap.groupByKey().flatMap(lambda x: getTopDenseN(x, 2)).collect(): print(i)

