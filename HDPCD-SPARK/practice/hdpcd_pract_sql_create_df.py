from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf =  SparkConf().setMaster("local").setAppName("MyAPPName")
sc =  SparkContext(conf=conf)
sqlContext =  SQLContext(sc)

# NOTE: local path doesn't work 
#df = sqlContext.read.json("/home/hduser/RAJ/Spark/HDPCD/Input-Data/people.json")

# give filesyatem path

df = sqlContext.read.json("/hdpcd/people.json")

df.show()
''''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+


''''
