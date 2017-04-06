# Hadoop Certification - CCA - Scala - Reading and Saving Text Files

## call the shell
[ma186082@quickstart ~]$ spark-shell --master local[2];

scala> val dataRDD=sc.textFile("/user/hive/warehouse/retail_db.db/orders")

scala> dataRDD.count()
res1: Long = 68883

scala> dataRDD.take(3).foreach(println)
12013-07-25 00:00:00.011599CLOSED
22013-07-25 00:00:00.0256PENDING_PAYMENT
32013-07-25 00:00:00.012111COMPLETE

scala> dataRDD.saveAsTextFile("ss/retail_db.db/orders/txt")
[Stage 2:>                                                          (0 + 0) /  
scala> dataRDD.saveAsObjectFile("ss/retail_db.db/orders/obj")
[Stage 3:>                                                          (0 + 0) / 
[Stage 3:============================================>              (3 + 1) /      
