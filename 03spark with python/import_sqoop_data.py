from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("myPyspark")
sc = SparkContext(conf=conf)
dataRDD = sc.textFile("/user/ma186082/sqoop_import/departments")
for line  in dataRDD.collect():
        print(line)
dataRDD.saveAsTextFile("/user/ma186082/pyspark/departments")

