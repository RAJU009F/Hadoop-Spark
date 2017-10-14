from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)

sqlContext =  SQLContext(sc)

#usersDF =  sqlContext.read.load("/hdpcd/users.parquet")
#usersDF.select("name", "favorite_color").write.save("/hdpcd/output/namesAndFavColors.parquet")

peopleDF =  sqlContext.read.load("/hdpcd/people.json", format="json")
#peopleDF.select(peopleDF.name, peopleDF.age).write.save("/hdpcd/output/namesAndAges", format="parquet")

peopleDF.registerTempTable("people")
peopleDF.saveAsTable("people")

#sqlContext.sql("select * from people").collect()


# run direct sql queries on files in the filesyatem

sqlContext.sql("SELECT * from parquet.`/hdpcd/users.parquet`")

