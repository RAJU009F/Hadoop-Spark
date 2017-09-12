from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics

conf = SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)

seriesX = sc.parallelize([1.0,2.0, 3.0,3.0,5.0])
sereiesY = sc.parallelize([11.0,22.0,33,33,555])

print "Correlation is:"+ str(Statistics.corr(seriesX, sereiesY, method="pearson"))

print "Correlation is:"+ str(Statistics.corr(seriesX, sereiesY, method="spearman"))

data = sc.parallelize([np.array([1.0,10.0,100.0]), np.array([2.0, 20.0, 200.0]), np.array([5.0, 33.0, 366.0])])

print Statistics.corr(data, method="pearson")
print Statistics.corr(data, method="spearman")
