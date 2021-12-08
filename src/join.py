

from pyspark import SparkContext

sc = SparkContext("local", "Join app")

x = sc.parallelize([("spark", 1), ("hadoop", 4)])

y = sc.parallelize([("spark", 2), ("hadoop", 5)])

joined_rdd = x.join(y)

final = joined_rdd.collect() 

print ( "Joined RDD -> %s " % ( final ) )  
