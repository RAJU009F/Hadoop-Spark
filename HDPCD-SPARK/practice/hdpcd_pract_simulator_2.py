from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

lines = sc.textFile("/hdpcd/hadoopexamsim_01.txt")

words =  lines.flatMap(lambda line: line.split(","))

wordcount = words.count()
print "=======================    :::: ",wordcount
hadoopwords = words.filter(lambda word: "hadoop" in  word.lower())

print hadoopwords.count()



