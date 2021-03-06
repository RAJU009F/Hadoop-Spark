from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
textRDD =  spark.read.option('format', 'text').load("/p01/trade/trade_data.txt")
textRDD.collect().show()
    
