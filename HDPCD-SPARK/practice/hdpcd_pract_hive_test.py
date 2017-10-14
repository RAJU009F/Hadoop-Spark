from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext


conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

sqlContext = HiveContext(sc)

sqlContext.sql("CREATE TABLE IF NOT EXISTS test_hive(key INT, value STRING)")
sqlContext.sql("LOAD DATA LOCAL INPATH `/hdpcd/kv1.txt` INTO TABLE test_hive")


results = sqlContext.sql("FROM test_hive SELECT key, value").collect()

print results


