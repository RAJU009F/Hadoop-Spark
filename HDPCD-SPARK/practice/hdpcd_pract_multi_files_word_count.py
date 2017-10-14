from pyspark import SparkContext, SparkConf

conf  =  SparkConf().setMaster("local").setAppName("MyApp")

sc =  SparkContext(conf=conf)

# create RDDS for each file
Rdd1 = sc.textFile("/hdpcd/file1.txt")
Rdd2 = sc.textFile("/hdpcd/file2.txt")
Rdd3 = sc.textFile("/hdpcd/file3.txt")

# concatenate rdds
Rdd12 = Rdd1.union(Rdd2)
Rdd123 = Rdd12.union(Rdd3)

print Rdd123.collect()


# word count 

mapRDD =  Rdd123.flatMap(lambda line:line.split(" "))
print mapRDD.collect()
count = mapRDD.count()
print "No of words:",count
