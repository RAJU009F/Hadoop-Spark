from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf  =  SparkConf().setAppName("trade app").setMaster("local[*]")

sc = SparkContext(conf=conf)

traderdd =  sc.textFile("/p01/trade/trade_data.txt")

eventspertrade = traderdd.map(lambda trade :  trade.split("|"))
allevents = traderdd.flatMap(lambda trade :  trade.split("|"))

'''
'''
# 1.	What are the total number of events?
eventscount  =  allevents.count()


# 2.	What are the distinct product types?
distinctproducttypes  =  allevents.map(lambda e: e.split(",")[0]).distinct().collect()


#3.	For each product type, what is the total amount?
productsandTotalprice =  allevents.map(lambda e: (e.split(",")[0], float(e.split(",")[2])) ).reduceByKey(lambda a, b: a+b).collect()


#4.	Can we see the events ordered by the time they occurred?
eventsOrderbytime = allevents.map(lambda e: (e.split(",")[1], e) ).sortByKey().collect()


#5.	For a given event, get the related events which make up the trade?
tradewithid  = traderdd.zipWithIndex().flatMap(lambda x:  [( x[1], v) for v in x[0].split('|')]).groupByKey().filter(lambda s: u' FWD,29052016:15:40,23.20' in s[1]).collect()




productprice = traderdd.zipWithIndex().flatMap(lambda x:  [( x[1], v.split(',')[0],v.split(',')[2], float(v.split(',')[2])) for v in x[0].split('|')]).map(lambda e: (e[1], e[3]))
#7.	How do we get the minimum amount for each product type?
productswithminprice = productprice.reduceByKey(min).collect()

#8.	How do we get the maximum amount for each product type?
productswithmaxprice = productprice.reduceByKey(max).collect()


#9.	How do we get the total amount for each trade, and the max/min across trades?
tradeprice  =  traderdd.zipWithIndex().flatMap(lambda x:  [( x[1], v.split(',')[0],v.split(',')[2], float(v.split(',')[2])) for v in x[0].split('|')]).map(lambda e: (e[0], e[3]))
totalpricepertrade =  tradeprice.reduceByKey(lambda a, b: a+b).collect()
minpricepertrade =  tradeprice.reduceByKey(min).collect()
maxpricepertrade =  tradeprice.reduceByKey(max).collect()

print "Total Events :{}".format(eventscount)
print "Distinct Product Types :{}".format(distinctproducttypes)
print "Product Type  with total cost :{}".format(productsandTotalprice)
print "Events Ordered by the Time they occured :{}".format(eventsOrderbytime)
print "related Events : {}".format(tradewithid)
print "Products with min price:{}".format(productswithminprice)
print "Products with max price:{}".format(productswithmaxprice)

print "All trades with total price:{}".format(totalpricepertrade)
print "Tades with max price:{}".format(maxpricepertrade)
print "Trades with min price:{}".format(minpricepertrade)

#6.	Data needs to be saved into Hive for other consumers?
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

df =  traderdd.zipWithIndex().flatMap(lambda x:  [( x[1], v.split(',')[0],v.split(',')[2], v.split(',')[2]) for v in x[0].split('|')])
hasattr(df, "toDF")
spark = SparkSession(sc)
hasattr(df, "toDF")
df.toDF(['trade_id', 'product', 'timestamp', 'price']).show()
df.toDF(['trade_id', 'product', 'timestamp', 'price']).write.saveAsTable("trade_events")



