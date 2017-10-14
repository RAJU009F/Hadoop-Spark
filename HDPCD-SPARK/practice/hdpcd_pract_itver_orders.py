from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")

sc = SparkContext(conf=conf)

orders =  sc.textFile("/hdpcd/retail_db/orders/part-00000")

'''
ORDERS structure 
================

order_id,  date,  cust_id, order_status


'''
'''print orders.first()
print type(orders)
print orders.count()
'''

# get the status column and get the distict 

#orderStatus =  orders.map(lambda order: ("COMPLETE" in  order.split(",")[3]) or ("CLOSED" in order.split(",")[3]))
orderStatus =  orders.filter(lambda order: ("COMPLETE" in  order.split(",")[3]) or ("CLOSED" in order.split(",")[3]))



#for order in orderStatus.collect():
#	print order
	
	
# Test Accumulators

ordersCompletedCount = sc.accumulator(0)

def isCompleted(order, ordersCompletedCount ):
	if ("COMPLETE" in  order.split(",")[3]) or ("CLOSED" in order.split(",")[3]):
		ordersCompletedCount.add(1)
		return True
	else:
		return False
		
orderMap =  orders.filter(lambda order: isCompleted(order,ordersCompletedCount ))			
print orderMap.count()
print ordersCompletedCount.value
'''
30455
30455

'''
# get the count the nof orders per status

orderStatusCount =  orders.map(lambda order:  (order.split(",")[3], 1)).reduceByKey(lambda x,y:x+y)

#for (status , count ) in orderStatusCount.collect():
#	print "status:{0}, count:{1}".format(status , count )

'''
status:COMPLETE, count:22899
status:PAYMENT_REVIEW, count:729
status:PROCESSING, count:8275
status:CANCELED, count:1428
status:PENDING, count:7610
status:CLOSED, count:7556
status:PENDING_PAYMENT, count:15030
status:SUSPECTED_FRAUD, count:1558
status:ON_HOLD, count:3798

'''



#  join() fullouter join

orderItems =  sc.textFile("/hdpcd/retail_db/order_items/part-00000")

orderItemMap = orderItems.map(lambda orderItem: (int(orderItem.split(",")[1]),(int(orderItem.split(",")[2]), float(orderItem.split(",")[5]))))

ordersMap = orders.filter(lambda order: order.split(",")[3]=="COMPLETE" or order.split(",")[3]=="CLOSED").map(lambda order : (int(order.split(",")[0]),order.split(",")[1]))


ordersJoin = ordersMap.join(orderItemMap)

'''for item in ordersJoin.take(4):
	print item'''

'''
(4, (u'2013-07-25 00:00:00.0', (897, 24.99)))
(4, (u'2013-07-25 00:00:00.0', (365, 59.99)))
(4, (u'2013-07-25 00:00:00.0', (502, 50.0)))
(4, (u'2013-07-25 00:00:00.0', (1014, 49.98)))


'''


# leftOuterJoin
'''
ordersLefteOuterJoin = ordersMap.leftOuterJoin(orderItemMap).filter(lambda item: item[1][1]==None)

for item in ordersLefteOuterJoin.take(10): print item
'''
'''
(6, (u'2013-07-25 00:00:00.0', None))
(22, (u'2013-07-25 00:00:00.0', None))
(26, (u'2013-07-25 00:00:00.0', None))
(32, (u'2013-07-25 00:00:00.0', None))
(76, (u'2013-07-25 00:00:00.0', None))
(80, (u'2013-07-25 00:00:00.0', None))
(90, (u'2013-07-25 00:00:00.0', None))
(102, (u'2013-07-25 00:00:00.0', None))
(126, (u'2013-07-26 00:00:00.0', None))
(210, (u'2013-07-26 00:00:00.0', None))

'''
#  daily revenue per products 
'''
ordersJoinMap = ordersJoin.map(lambda r: ((r[1][0], r[1][1][0]),r[1][1][1]))
dailyRevenenuePerProductId = ordersJoinMap.reduceByKey(lambda total, revenue: total+revenue)
for item in dailyRevenenuePerProductId.take(10): print item
'''
'''
((u'2014-02-27 00:00:00.0', 93), 24.99)
((u'2014-03-13 00:00:00.0', 191), 1199.8799999999999)
((u'2014-01-11 00:00:00.0', 191), 2499.75)
((u'2014-03-26 00:00:00.0', 1073), 3799.8100000000004)
((u'2014-01-16 00:00:00.0', 728), 65.0)
((u'2014-07-14 00:00:00.0', 1014), 1049.5800000000002)
((u'2014-05-03 00:00:00.0', 278), 44.99)
((u'2014-06-21 00:00:00.0', 1073), 1199.94)
((u'2014-01-17 00:00:00.0', 725), 108.0)
((u'2014-04-04 00:00:00.0', 502), 1200.0)

'''


# daily revenue using broadcast variable

products = open("/home/hduser/RAJ/Spark/HDPCD/Input-Data/retial_db/products/part-00000").read().splitlines()
productsMap = dict(map(lambda product: (int(product.split(",")[0]),product.split(",")[2]), products))

productsBV = sc.broadcast(productsMap)

#print productsBV.value
ordersJoin.saveAsTextFile("/hdpcd/output/orders_orderItemsJoin")

	
