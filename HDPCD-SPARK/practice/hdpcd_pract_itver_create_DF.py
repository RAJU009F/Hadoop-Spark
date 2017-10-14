from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc =  SparkContext(conf=conf)

sqlContext = SQLContext(sc)

ordersRDD =  sc.textFile("/hdpcd/retail_db/orders/part-00000")

# NOTE:: create DataFrame using toDF()
#ordersDF = ordersRDD.map(lambda rec: Row(order_id=int(rec.split(",")[0]), order_date=rec.split(",")[1], order_customer_id=int(rec.split(",")[2]), order_status=rec.split(",")[3])).toDF()
#ordersDF.show()

'''
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
|              918|2013-07-25 00:00:...|      11| PAYMENT_REVIEW|
|             1837|2013-07-25 00:00:...|      12|         CLOSED|
|             9149|2013-07-25 00:00:...|      13|PENDING_PAYMENT|
|             9842|2013-07-25 00:00:...|      14|     PROCESSING|
|             2568|2013-07-25 00:00:...|      15|       COMPLETE|
|             7276|2013-07-25 00:00:...|      16|PENDING_PAYMENT|
|             2667|2013-07-25 00:00:...|      17|       COMPLETE|
|             1205|2013-07-25 00:00:...|      18|         CLOSED|
|             9488|2013-07-25 00:00:...|      19|PENDING_PAYMENT|
|             9198|2013-07-25 00:00:...|      20|     PROCESSING|
+-----------------+--------------------+--------+---------------+

'''

ordersSchema =  ordersRDD.map(lambda rec: Row(order_id=int(rec.split(",")[0]), order_date=rec.split(",")[1], order_customer_id=int(rec.split(",")[2]), order_status=rec.split(",")[3] ))
ordersDF = sqlContext.createDataFrame(ordersSchema)

#ordersDF.show()

'''
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
|              918|2013-07-25 00:00:...|      11| PAYMENT_REVIEW|
|             1837|2013-07-25 00:00:...|      12|         CLOSED|
|             9149|2013-07-25 00:00:...|      13|PENDING_PAYMENT|
|             9842|2013-07-25 00:00:...|      14|     PROCESSING|
|             2568|2013-07-25 00:00:...|      15|       COMPLETE|
|             7276|2013-07-25 00:00:...|      16|PENDING_PAYMENT|
|             2667|2013-07-25 00:00:...|      17|       COMPLETE|
|             1205|2013-07-25 00:00:...|      18|         CLOSED|
|             9488|2013-07-25 00:00:...|      19|PENDING_PAYMENT|
|             9198|2013-07-25 00:00:...|      20|     PROCESSING|
+-----------------+--------------------+--------+---------------+
only showing top 20 rows


'''


#  Note ::  create DF using sqlContext.createDataFrame
ordersDF.registerTempTable("ORDERS")
# get the total number of orders per customer
custOrders = sqlContext.sql("SELECT count(*) as order_count, order_customer_id from ORDERS group by order_customer_id  having order_count>10 order by order_count desc ")

#custOrders.show()

custOrders.write.save("/hdpcd/output/customerOrdersCount", "json")




