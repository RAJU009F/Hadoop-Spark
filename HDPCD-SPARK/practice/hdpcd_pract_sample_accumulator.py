from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
conf =  SparkConf().setAppName("MyAPp").setMaster("local")
sc = SparkContext(conf=conf)

'''
Example:  accumulator variable

desinged to used in parallel environement

'''
accum =  sc.accumulator(0)
rdd =  sc.parallelize(range(1,1000))


def accumuAdd(x):
	accum.add(x)

mulRdd =  rdd.map(lambda param: param**2)

#for item in mulRdd.take(5):

mulRdd.foreach(accumuAdd)
print accum
