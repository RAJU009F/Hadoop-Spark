from pyspark import SparkContext, SparkConf

conf =  SparkConf().setMaster("local").setAppName("My App")
sc =  SparkContext(conf=conf)


data =  sc.textFile("/hdpcd/log.txt")
'''
# word count with reduceByKey
words = data.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
groupedWords = words.reduceByKey(lambda x, y: x+y)
counts = groupedWords.take(5)

#counts.show()
for pair in counts:
	print pair
'''	
# word count with groupByKey

words =  data.flatMap(lambda line: line.split(" "))

groupedWords =  words.groupByKey().collect()

#counts = groupedWords().count()

groupedWords.show()


	
