from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

sqlContext =  SQLContext(sc)

tutorialRDD =  sc.textFile("/hdpcd/hadoopexam02.txt")
fieldsRDD = tutorialRDD.map(lambda l:l.split(","))
columnsRDD = fieldsRDD.map(lambda line:(line[0], line[1], line[2].strip()))

schemaString = "tutorial price tax"

fields  = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

df = sqlContext.createDataFrame(columnsRDD, schema)

df.registerTempTable("hadoop")

results =  sqlContext.sql("SELECT * from hadoop")

#names = results.map(lambda p:  ("Name: " + p.tutorial+"tax"+str(p.tax)))

for item in results.collect():
	print(item)
	
'''
Row(tutorial=u'Hadoop', price=u'2900', tax=u'15')
Row(tutorial=u'Spark', price=u'3500', tax=u'14')
Row(tutorial=u'AWS', price=u'2700', tax=u'13')
Row(tutorial=u'Azure', price=u'2800', tax=u'11')
Row(tutorial=u'JAva', price=u'3000', tax=u'16')
Row(tutorial=u'HBase', price=u'3200', tax=u'20')


'''	
	
