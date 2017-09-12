from pyspark import SparkContext, SparkConf
import random
conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

NUM_SAMPLES = 500

def inside(p):
	x, y = random.random(), random.random()
	return x*x + y*y <1

count = sc.parallelize(xrange(0, NUM_SAMPLES)).filter(inside).count()
print "Pi  is roughly %f" %(4.0*count/NUM_SAMPLES)

