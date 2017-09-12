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


blocks =  sc.parallelize([((0,0, Matrices.dense(3,2,[1,2,3,4,5,6])),
							((1,0), Matrices.dense(3,2,[7,8,9,10,11,12])))])
							
mat = BlockMatrix(blocks,3,2)
m = mat.numRows()
n = mat.numCols()

blockRDD  = mat.blocks

localMat = mat.toLocalMatrix()

indexedRowMat = mat.toIndexedRowMatrix()
coordinateMat = mat.toCoordinateMatrix()



							
