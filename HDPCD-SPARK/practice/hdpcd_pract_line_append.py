from pyspark import SparkContext, SparkConf

conf  =  SparkConf().setMaster("local").setAppName("MyApp")

sc =  SparkContext(conf=conf)

lines  = sc.textFile("/hdpcd/hadoopexam02.txt")
print lines.collect()

# append the total price to each line

lines = lines.map(lambda line : line+","+format(float(line.split(",")[1])+float(line.split(",")[1])*float(line.split(",")[2])/100))

print lines.collect()

# save data

lines.saveAsTextFile("/hdpcd/output/hadoopexam02.txt")


