from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession

from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry

conf =  SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf=conf)

entries = sc.parallelize([MatrixEntry(0,0,1.2),MatrixEntry(1,0,2.1), MatrixEntry(6,1,3.7)])
entries = sc.parallelize([(0,0,1.2),(1,0,2.1),(2,1,3.7)])

mat = CoordinateMatrix(entries)
m = mat.numRows()
n = mat.numCols()

# Get the entries as an RDD of MatrixEntries
entriesRDD = mat.entries

# convert to RowMatrix
rowMat = mat.toRowMatrix()

# Convert to an IndexedRowMatrix
indexedRowMat = mat.toIndexedRowMatrix()

# convert to a BlockMatrix
blockMat = mat.toBlockMatrix()



