
# returns list of elemens in the rdd

from pyspark import SparkContext 

sc = SparkContext("local", "Count app") 

words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
) 

collectionsOfWords = words.collect() 

print ("Elements in RDD -> %s ", collectionsOfWords ) 




