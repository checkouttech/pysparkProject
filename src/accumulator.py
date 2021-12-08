

from pyspark import SparkContext 

sc = SparkContext("local", "Accumulator app") 

num = sc.accumulator(0) 

def f(x): 
   print "RDD value passed -> %s " %(x) 
   global num 
   num += x 

numbers = sc.parallelize([20,30,40,50])

numbers.foreach(f) 

total= num.value 

print ("Sum total is -> %i " % (total)) 






