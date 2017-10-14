from pyspark.sql import SQLContext, Row

from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

sqlContext =  SQLContext(sc)

lines = sc.textFile("/hdpcd/hadoopexam02.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(tutorial=p[0], price=int(p[1]), tax=int(p[2])))

schemaPeople = sqlContext.createDataFrame(people)
print schemaPeople.collect()
schemaPeople.registerTempTable("hadoop")


priceList = sqlContext.sql("SELECT tutorial, price FROM hadoop WHERE price>=2500 AND price<=3000")
#tutorialnames = priceList.map(lambda p: "tutorial"+p.tutorial)
for name in priceList.collect():
	print name
'''
Row(tutorial=u'Hadoop', price=2900)
Row(tutorial=u'AWS', price=2700)
Row(tutorial=u'Azure', price=2800)
Row(tutorial=u'JAva', price=3000)



'''
