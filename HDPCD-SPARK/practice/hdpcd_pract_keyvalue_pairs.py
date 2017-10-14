from pyspark import SparkContext,  SparkConf

conf  =  SparkConf().setMaster("local").setAppName("MyApp")

sc =  SparkContext(conf=conf)

lines =  sc.textFile("/hdpcd/hadoopexam01.txt")
pairs =  lines.map(lambda s: (s,1))
counts  =  pairs.reduceByKey(lambda a, b: a+b)

print counts.collect()

sortedlist =  counts.sortByKey()

print sortedlist.collect()


