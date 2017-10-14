from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, SQLContext
conf =  SparkConf().setAppName("MyAPp").setMaster("local")
sc = SparkContext(conf=conf)

'''
Example:  Broad cast variable
Read only variabe
desinged for scalability
only mapped once to worker node
'''
MULTIPLICATION_FACTOR = 5
FACTOR = sc.broadcast(MULTIPLICATION_FACTOR)
rdd =  sc.parallelize(range(1,1000))

mulRdd =  rdd.map(lambda param: param*FACTOR.value)

for item in mulRdd.take(5):
	print item
