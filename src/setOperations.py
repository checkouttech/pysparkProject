
from pyspark import SparkContext

sc = SparkContext("local", "spark operation") 
sc.setLogLevel("ERROR")


numbers_rdd = sc.parallelize ( [4, 8, 2, 2, 4, 7, 0, 3, 3, 9, 2, 6, 0, 0, 1, 7, 5, 1, 9, 7] ) 

# union(), intersection(), subtract(), or cartesian().
# cache 
# lazy 















