from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf 

conf =  SparkConf().setAppName("MyApp").setMaster("local")
sc =  SparkContext(conf=conf)
sqlContext = SQLContext(sc)

# NOTE:  not working becoz 
#df = sqlContext.sql("SELECT * FROM retail_db.orders")

df.show()
