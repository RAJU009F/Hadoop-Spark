from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
conf =  SparkConf().setAppName("MyAPp").setMaster("local")
sc = SparkContext(conf=conf)


sqlContext  =  SQLContext(sc)

df  =  sqlContext.createDataFrame(list(("Hitler",1)))

df.registerTempTable("people")

recodrs = df.select("SELECT * from people")

print records.collect()

