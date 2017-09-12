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

# distributed indexed Row matrix

conf =  SparkConf().setMaster("local").setAppName("MyAPP")
sc = SparkContext(conf = conf)

indexedRows =  sc.parallelize([IndexedRow(0, [1,2,3]),
								IndexedRow(1,[4,5,6]),
								IndexedRow(2,[7,8,9]),
								IndexedRow(3,[10,11,12])])
								
indexedRows =  sc.parallelize([(0, [1,2,3]),
								(1,[4,5,6]),
								(2, [7,8,9]),
								(3, [10,11,12])])
								
mat =  IndexedRowMatrix(indexedRows)	

m = mat.numRows()
n = mat.numCols()	

rowsRDD = mat.rows

rowMat = mat.toRowMatrix()														
