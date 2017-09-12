from pyspark import SparkContext, SparkConf

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

''' Shared variables are of two
 1. Broadcast : read-only variable cached on each machine to redcue the communication cost
		Spark actions are executed through a set of stages, separated by distributed “shuffle” operations.
		Spark automatically broadcasts the common data needed by tasks within each stageg
 2. accumulator: Accumulators are variables that are only "added" to through an associative and commutative operation and can therefore be efficiently supported in parallel. 
'''

# brodcast varibale
broadcastVar = sc.broadcast([1,2,3])
print " broadcast variable :"
print " broadcast value: ", broadcastVar.value
print "broadcast id: ", broadcast.id


#accumulator variable

accum = sc.accumulator(0)
print "accumulator :"
print "accumulator value:", accum.value
print "accumulator id:", accum.id


