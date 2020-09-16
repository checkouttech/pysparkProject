

from pyspark import SparkContext 
sc = SparkContext("local", "Accumulator app") 
num = sc.accumulator(0) 
def f(x): 
   print "RDD value passed -> %s " %(x) 

   global num 
   num+=x 
rdd = sc.parallelize([20,30,40,50]) 
rdd.foreach(f) 
final = num.value 
print "Accumulated value is -> %i" % (final)






