from pyspark import SparkContext, SparkConf

conf =  SparkConf().setMaster("local").setAppName("MyApp")
sc =  SparkContext(conf=conf)

lines  =  sc.textFile("/hdpcd/log.txt")
words  =  lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda x,y:x+y)

for pair in words.sortByKey().collect():
	print pair




