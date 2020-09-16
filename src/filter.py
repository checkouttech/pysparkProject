
from pyspark import SparkContext
sc = SparkContext("local", "Filter app")
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


words_filter =  words.filter(lambda x: 'spark' in x) 
filtered =  words_filter.collect() 

print " words_filter type -> %s" % (type(words_filter)) 

print " filtered type -> %s" % (type(filtered)) 

print "words_filter > %s" % (words_filter) 

print "Fitered RDD -> %s" % (filtered)

#for  x in filtered: 
#    print ( x ) 




