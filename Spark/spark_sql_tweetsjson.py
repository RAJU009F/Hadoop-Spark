from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext, Row

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

hiveCtx =  HiveContext(sc)

input = hiveCtx.read.json("/p01/sample_data/tweets.json")
input.registerTempTable("tweets")
topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetcount LIMIT 10")
print topTweets.collect()
