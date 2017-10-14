from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
conf =  SparkConf().setAppName("MyAPp").setMaster("local")
sc = SparkContext(conf=conf)

rdd = sc.textFile("/hdpcd/ml-100k/u.item")
print rdd.first()
