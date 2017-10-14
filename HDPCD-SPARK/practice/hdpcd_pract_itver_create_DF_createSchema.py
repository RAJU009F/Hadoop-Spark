from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext, Row

conf = SparkConf().setMaster("local").setAppName("MyApp")
sc =  SparkContext(conf=conf)

sqlContext = HiveContext(sc)

productsRaw =  open("/home/hduser/RAJ/Spark/HDPCD/Input-Data/retial_db/products/part-00000").read().splitlines()
productsRDDD = sc.parallelize(productsRaw)

productsDF = productsRDDD.map(lambda rec :  Row(product_id=int(rec.split(",")[0]), product_name=rec.split(",")[2])).toDF()

#productsDF.show()

"""
+----------+--------------------+
|product_id|        product_name|
+----------+--------------------+
|         1|Quest Q64 10 FT. ...|
|         2|Under Armour Men'...|
|         3|Under Armour Men'...|
|         4|Under Armour Men'...|
|         5|Riddell Youth Rev...|
|         6|Jordan Men's VI R...|
|         7|Schutt Youth Recr...|
|         8|Nike Men's Vapor ...|
|         9|Nike Adult Vapor ...|
|        10|Under Armour Men'...|
|        11|Fitness Gear 300 ...|
|        12|Under Armour Men'...|
|        13|Under Armour Men'...|
|        14|Quik Shade Summit...|
|        15|Under Armour Kids...|
|        16|Riddell Youth 360...|
|        17|Under Armour Men'...|
|        18|Reebok Men's Full...|
|        19|Nike Men's Finger...|
|        20|Under Armour Men'...|
+----------+--------------------+
only showing top 20 rows


"""


productsDF.registerTempTable("PRODUCTS")

#sqlContext.sql("select * from PRODUCTS").show()

'''
+----------+--------------------+
|product_id|        product_name|
+----------+--------------------+
|         1|Quest Q64 10 FT. ...|
|         2|Under Armour Men'...|
|         3|Under Armour Men'...|
|         4|Under Armour Men'...|
|         5|Riddell Youth Rev...|
|         6|Jordan Men's VI R...|
|         7|Schutt Youth Recr...|
|         8|Nike Men's Vapor ...|
|         9|Nike Adult Vapor ...|
|        10|Under Armour Men'...|
|        11|Fitness Gear 300 ...|
|        12|Under Armour Men'...|
|        13|Under Armour Men'...|
|        14|Quik Shade Summit...|
|        15|Under Armour Kids...|
|        16|Riddell Youth 360...|
|        17|Under Armour Men'...|
|        18|Reebok Men's Full...|
|        19|Nike Men's Finger...|
|        20|Under Armour Men'...|
+----------+--------------------+

'''
## join ORDERS ANd ORDER_ITEMS


sqlContext.sql("use raju_retail_db_txt")
sqlContext.sql("show tables")

