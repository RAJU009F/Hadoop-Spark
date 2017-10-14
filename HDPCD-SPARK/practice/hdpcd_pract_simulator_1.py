from pyspark import SparkContext, SparkConf

conf =  SparkConf().setMaster("local").setAppName("MyAPP")
sc =  SparkContext(conf=conf)

'''
fileRdd1 =  sc.textFile("/hdpcd/dir5/hadoopexamA.txt")
fileRdd1 = fileRdd1.map(lambda x: x.split())

fileRdd2 =  sc.textFile("/hdpcd/dir5/hadoopexamB.txt")
fileRdd2 = fileRdd2.map(lambda x: x.split())

fileRdd3 = sc.textFile("/hdpcd/dir5c/hadoopexamC.txt")
fileRdd3 = fileRdd3.map(lambda x: x.split())
print fileRdd1.collect()
print "==================", fileRdd2.collect()
fileRdd12 = fileRdd1.union(fileRdd2)
fileRdd123 = fileRdd12.union(fileRdd3)
print fileRdd123.collect()
hadoopwords = fileRdd123.filter(lambda x: "Hadoop" in x)
print "====================================="
print hadoopwords.collect()
print "====================================="
hadoopwords.persist()
'''
acc =  []
fileRdd1  =  sc.textFile("/hdpcd/dir5/hadoopexamA.txt")
wordsRdd1 = fileRdd1.map(lambda line: line.split(" ") )

fileRdd2 =  sc.textFile("/hdpcd/dir5c/hadoopexamC.txt")

wordsRdd2 = fileRdd2.map(lambda line: line.split(" "))



fileRdd3  =  sc.textFile("/hdpcd/dir5/hadoopexamA.txt")
wordsRdd3 = fileRdd3.map(lambda line: line.split(" ") )


print wordsRdd1.collect()
print wordsRdd2.collect()
print wordsRdd3.collect()


wordsRdd123 = wordsRdd1.union(wordsRdd2).union(wordsRdd3)
fileRdd123 = fileRdd1.union(fileRdd2).union(fileRdd3)
#hadoopwords = wordsRdd123.reduce(lambda acc, lis2: acc.extend(lis2)).filter(lambda word: "Hadoop" in word )

#print wordsRdd123.collect()
print fileRdd123.collect()

words =  fileRdd123.flatMap(lambda word: word.split(" ") )
hadoopwords = words.filter(lambda word: "hadoop" in word)

wordsRdd1.cache()
wordsRdd2.cache()
print words.collect()

print hadoopwords.collect()




