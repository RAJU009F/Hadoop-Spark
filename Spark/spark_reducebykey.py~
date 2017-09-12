from pyspark import SparkContext, SparkConf

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

fileRDD = sc.textFile("/p01/sample_data/text_data.txt")
mapRDD = fileRDD.flatMap(lambda line: line.split(" ")).map(lambda x: (x ,1))

# aggregate ops
aggregateRDD = mapRDD.distinct()
print mapRDD.distinct().count()

# sort
mapRDD = mapRDD.sortByKey()
# map groupByKey 
aggregateRDD = mapRDD.groupByKey()
for item in aggregateRDD.take(10): 
	print item

# reduceBy key
aggregateRDD = mapRDD.sortByKey(True ).reduceByKey(lambda x ,y : x+y)
for item in aggregateRDD.take(10): 
	print item


# save file in text and sequence mode
#aggregateRDD.saveAsSequenceFile("/p01/spark_data/sequenceFile")

#aggregateRDD.saveAsTextFile("/p01/spark_data/texteFile")

# join
joinRDD = aggregateRDD.join(aggregateRDD)
for item in joinRDD.take(10):
	print item


# takeSample

for item in aggregateRDD.takeSample('withReplacement', 10):
	print item



# saveAsSequenceFile
#joinRDD.saveAsSequenceFile("/p01/spark_data/squenceFile")

#joinRDD.saveAsSequenceFile("/p01/spark_data/squenceFile")

# countByKey
print aggregateRDD.countByKey()



