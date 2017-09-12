from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Matrices, Vectors
conf = SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)


parallelData =  sc.parallelize([0.1, 0.15, 0.2, 0.3, 0.25])

testResult = Statistics.kolmogorovSmirnovTest(parallelData, "norm", 0,1 )

print testResult


