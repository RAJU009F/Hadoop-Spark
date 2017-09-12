from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row
from pyspark.sql import SQLContext, Row

conf  = SparkConf().setMaster("local").setAppName("My APP")
sc =  SparkContext(conf=conf)

df = SQLContext.createDataFrame(data, ["label", "features"])
lr = LogisticRegression(maxITer=10)

model = lr.fit(df)

model.transform(df).show()


