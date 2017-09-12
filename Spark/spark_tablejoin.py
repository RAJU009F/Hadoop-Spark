from pyspark import SparkContext, SparkConf

conf  = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)
orderRDD = sc.textFile()
orderItemsRDD = sc.textFile()
