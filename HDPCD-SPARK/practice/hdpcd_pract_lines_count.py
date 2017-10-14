from pyspark import SparkConf
from pyspark import SparkContext

conf  = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf=conf)

#  create RDD of hadoopexam01.txt
linesRDD =  sc.textFile("/hdpcd/hadoopexam01.txt")

# count the nof of lines which contains the HadoopExam
hadoopExamLinesCount = linesRDD.filter(lambda line: 'HadoopExam' in line).count()

# count the nof  of lines which contains doesn't contain the HadoopExam
hadoopExamNotLinesCount = linesRDD.filter(lambda line: 'HadoopExam' not in line).count()

print "HadoopExamLinesCount: ",hadoopExamLinesCount
print "hadoopExamNotLinesCount",hadoopExamNotLinesCount


