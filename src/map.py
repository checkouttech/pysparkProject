from pyspark import SparkContext
sc = SparkContext("local", "Map app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "hadoop", 
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

words_map = words.map(lambda x: (x, 1))

mapping = words_map.collect()

print "Key value pair -> %s" % (mapping)




