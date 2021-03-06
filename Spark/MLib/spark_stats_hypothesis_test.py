from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np

from pyspark.mllib.stat import Statistics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Matrices, Vectors
conf = SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)

# Hypothesis testing


vec = Vectors.dense(0.1,0.15,0.2,0.3,0.25)

goodnessOfFitTestResult = Statistics.chiSqTest(vec)
print "%s\n" %goodnessOfFitTestResult

mat = Matrices.dense(3,2, [1.0, 3.0, 5.0, 2.0, 4.0, 6.0])

independenceTestResult = Statistics.chiSqTest(mat)

print "%s \n" %independenceTestResult 

obs  = sc.parallelize([ LabeledPoint(1.0, [1.0,0.0,3.0]), LabeledPoint(1.0, [1.0,2.0,0.0]),LabeledPoint(1.0, [-1.0, 0.0, -0.5])])

featureTestResults = Statistics.chiSqTest(obs)

for i, result in enumerate(featureTestResults):
	print("Column %d: \n%s"%(i+1, result))
	
	

