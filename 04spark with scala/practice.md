# Hadoop Certification - CCA - Scala - Reading and Saving Text Files

## call the shell
[ma186082@quickstart ~]$ spark-shell --master local[2];

## read and play with file contents
scala> val dataRDD=sc.textFile("/user/hive/warehouse/retail_db.db/orders")

scala> dataRDD.count()
res1: Long = 68883

scala> dataRDD.take(3).foreach(println)
12013-07-25 00:00:00.011599CLOSED
22013-07-25 00:00:00.0256PENDING_PAYMENT
32013-07-25 00:00:00.012111COMPLETE

## save the RDD
scala> dataRDD.saveAsTextFile("ss/retail_db.db/orders/txt")
[Stage 2:>                                                          (0 + 0) /  
scala> dataRDD.saveAsObjectFile("ss/retail_db.db/orders/obj")
[Stage 3:>                                                          (0 + 0) / 
[Stage 3:============================================>              (3 + 1) / 


# Move data between HDFS and Spark – scala

## sequence file 

### write
scala> import org.apache.hadoop.io._
import org.apache.hadoop.io._
scala> dataRDD.map(x => (x.split("\\x01")(0),x.split("\\x01")(1))).saveAsSequenceFile("ss/retail_db.db/orders/seq/1")
import org.apache.hadoop.mapreduce.lib.output._

> val path="ss/retail_db.db/orders/seq/02"
path: String = ss/retail_db.db/orders/seq/02

scala> dataRDD.map(x => (new Text(x.split("\\x01")(0)), new Text(x.split("\\x01")(1)))).saveAsNewAPIHadoopFile(path, classOf[Text], classOf[Text], classOf[SequenceFileOutputFormat[Text, Text]])

### read
scala> sc.sequenceFile(path, classOf[IntWritable], classOf[Text]).map(rec => rec.toString()).take(3).foreach(println)
(1,2013-07-25 00:00:00.0)
(2,2013-07-25 00:00:00.0)
(3,2013-07-25 00:00:00.0)

