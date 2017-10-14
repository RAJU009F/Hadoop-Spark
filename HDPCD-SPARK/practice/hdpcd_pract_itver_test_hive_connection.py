from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc =  SparkContext(conf=conf)

sqlContext = HiveContext(sc)

sqlContext.sql("CREATE DATABASE IF NOT EXISTS  raju_test_database")
sqlContext.sql("use raju_test_database")
#sqlContext.sql("DROP DATABASE temp_table")

#sqlContext.sql(" SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product FROM orders o JOIN order_items oi ON o.order_id = oi.order_item_order_id JOIN products p ON p.product_id = oi.order_item_product_id WHERE o.order_status IN ('COMPLETE', 'CLOSED') GROUP BY o.order_date, p.product_name ORDER BY o.order_date, daily_revenue_per_product DESC").show()
sqlContext.sql("DROP TABLE raju_test_database.orders ")
sqlContext.sql(" CREATE  TABLE IF NOT EXISTS raju_test_database.orders( order_id int ,order_date string, order_customer_id int, order_status string) STORED AS orc ")
ordersRDD =  sc.textFile("/hdpcd/retail_db/orders/part-00000")
ordersSchema =  ordersRDD.map(lambda rec: Row(order_id=int(rec.split(",")[0]), order_date=rec.split(",")[1], order_customer_id=int(rec.split(",")[2]), order_status=rec.split(",")[3] ))
ordersDF = sqlContext.createDataFrame(ordersSchema)

ordersDF.write.insertInto("raju_test_database.orders")


