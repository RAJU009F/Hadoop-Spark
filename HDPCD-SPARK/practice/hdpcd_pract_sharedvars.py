from pyspark import SparkContext,  SparkConf

conf =  SparkConf().setMaster("local").setAppName("MyApp")

sc  =  SparkContext(conf=conf)

# broadcast variables
broadcastvar =  sc.broadcast([1,2,3])
broadcastvar.value
print "Braodcastor:",broadcastvar.value

# accumulator vars
accum = sc.accumulator(10)
sc.parallelize(range(1,10)).foreach(lambda x: accum.add(x))
print "Accumlator:",accum.value




