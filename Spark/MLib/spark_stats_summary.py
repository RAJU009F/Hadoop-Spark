from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics

conf  =  SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)

mat = sc.parallelize( [np.array([1.0,10.0,100.0]), np.array([2.0, 20, 200.0]),np.array([3.0, 30.0,300.0])])
summary = Statistics.colStats(mat)
print(summary.mean())
print(summary.variance())
print(summary.count())
print(summary.numNonzeros())

