from pyspark.sql import SQLContext, Row

from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

sqlContext =  SQLContext(sc)

data  =  sqlContext.read.load("/hdpcd/people.json","json")

data.registerTempTable("people")

sqlContext.udf.register("first_letter", lambda x: x[:03])

sqlContext.sql("SELECT first_letter(name) from people").show()

'''
+------------------+
|first_letter(name)|
+------------------+
|                 M|
|                 A|
|                 J|
|                 h|
+------------------+


+------------------+
|first_letter(name)|
+------------------+
|               Mic|
|               And|
|               Jus|
|               hit|
+------------------+




'''
