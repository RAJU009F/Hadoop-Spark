from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

conf =  SparkConf().setMaster("local").setAppName("MyAPPName")
sc =  SparkContext(conf=conf)
sqlContext =  SQLContext(sc)

df =  sqlContext.read.json("/hdpcd/people.json")

#df.show()
'''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
'''
#print schema

#df.printSchema()


'''
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

'''




#select the name column


df.select("name").show()

'''
+-------+
|   name|
+-------+
|Michael|
|   Andy|
| Justin|
+-------+


'''

# select everybody increment age
#df.select(df.name, df.age+1).show()
#df.select(df[name], df[age]+1).show()

'''
+-------+---------+
|   name|(age + 1)|
+-------+---------+
|Michael|     null|
|   Andy|       31|
| Justin|       20|
+-------+---------+

'''


# select people older than 21

#df.filter(df.age >21).show()


'''
+---+----+
|age|name|
+---+----+
| 30|Andy|
+---+----+



'''


#df.groupBy(df.age).count().show()
'''

+----+-----+
| age|count|
+----+-----+
|  19|    2|
|null|    1|
|  30|    1|
+----+-----+



'''

'''

df.groupBy(df.age).count().show()
'''

df.createOrReplaceTempView("people")

querydf = sqlContext.sql("SELECT * FROM people WHERE name='hitler'")


querydf.show()

''''
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
|  19| hitler|
+----+-------+

+---+------+
|age|  name|
+---+------+
| 19|hitler|
+---+------+



'''





