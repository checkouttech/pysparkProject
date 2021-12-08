

# apply map transformation on each element of RDD 

from pyspark import SparkContext 
from itertools import groupby 


sc = SparkContext("local", "map app") 

words = sc.parallelize ( 
           ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
            ) 


#words_map = words.map( lambda x : x.upper() ) 


words_map = words.map( lambda x : (x, 1 )  ) 



words_reduce = groupby(words_map , lambda p : p[0] ) 





print ( words_map.collect() )






