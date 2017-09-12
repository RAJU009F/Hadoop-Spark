from spark_session_config import spark

from pyspark.sql import Row

''' Schema merging example '''


sc =  spark.sparkContext

squareDF = spark.createDataFrame(sc.parallelize(range(1,6)).map(lambda i: Row(single=i, doubel=i**2)))
squareDF.write.parquet("/p01/spark_output/schema_merge_table/key=1")

cubesDF =  spark.createDataFrame(sc.parallelize(range(6,11)).map(lambda i: Row(single=i, doubel=i**3)))

cubesDF.write.parquet("/p01/spark_output/schema_merge_table/key=2")

mergedDF = spark.read.option("mergeSchema", "true").parquet("/p01/spark_output/schema_merge_table")
#mergedDF.printSchema()

'''
{
  "type" : "struct",
  "fields" : [ {
    "name" : "doubel",
    "type" : "long",
    "nullable" : true,
    "metadata" : { }
  }, {
    "name" : "single",
    "type" : "long",
    "nullable" : true,
    "metadata" : { }
  } ]
}
and corresponding Parquet message type:
message spark_schema {
  optional int64 doubel;
  optional int64 single;
}


'''

# READ Data  from merged table

parquetFile = spark.read.parquet("/p01/spark_output/merge_table")

parquetFile.createOrReplaceTempView("parquetFile")
data =  spark.sql("SELECT * FROM parquetFile ")
data.show()







