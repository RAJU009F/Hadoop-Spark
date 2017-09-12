from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext, Row

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

hiveCtx =  HiveContext(sc)

rows = hiveCtx.sql("SELECT key, value FROM mytable ")
keys = rows.map(lambda row:row[0])

