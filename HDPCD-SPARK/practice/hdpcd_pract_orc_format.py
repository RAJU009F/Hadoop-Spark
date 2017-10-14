from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SparkSQL
conf =  SparkConf().setAppName("MyAPp").setMaster("local")
sc = SparkContext(conf=conf)

hiveContext = HiveContext(sc)


