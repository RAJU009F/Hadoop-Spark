from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint


from pyspark.mllib.linalg import Matrix, Matrices

# Dense Matrix
dm2 = Matrices.dense(3,2,[1,2,3,4,5,6])

print dm2

# Sparse Matrix
sm = Matrices.sparse(3,2,[0,1,3],[0,2,1],[9,6,8])
print sm
