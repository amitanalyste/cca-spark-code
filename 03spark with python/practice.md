## Pyspark - Getting Started

# in spark-shell
spark-shell master local
spark-shell master yarn


val i: Int = 0
val num =1

val str: String = new String("hello man")
val abc = "abc"
println(str)
println(str+abc)


# copy hive-site 
sudo ln -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml

# run some sql on hive
scala> sqlContext.sql("show tables").collect().foreach(println)
[categories,false]
[customers,false]
[departments,false]

# retrieve data from hive and apply aggregation in spark
scala> sqlContext.sql("select * from departments").count()
res7: Long = 6


# Move data between HDF-S and Spark - pyspark


# Word count using pyspark


# Joining disparate data sets using pys park


# Aggregating data sets using pyspark - totals


# Aggregating data sets using pyspark - by key


# Filtering data using pyspark


# Sorting and Ranking using pyspark - global


# Sorting and Ranking using pyspark - by key


