import time 
from  timeit import timeit 
from datetime import datetime



from pyspark import SparkContext 


sc = SparkContext("local", "transformation app") 



# Narrow Transformation - All elements required are available in same partition 
# filter > 30 


# Wide Transformatino - Elements are spread through out different partitions 
# word count 

# lazy evaluation 


# create a sample list
my_list = [i for i in range(1,10000000)]

# parallelize the data
rdd_0 = sc.parallelize(my_list,3)
rdd_nopart = sc.parallelize(my_list)

rdd_1 = rdd_0.map(lambda x : x+4)


print ("-------->") 
# get the RDD Lineage 

print(rdd_1.toDebugString())



rdd_2 = rdd_0.map(lambda x : x+20) 

print ("-------->")
# get the RDD Lineage

print(rdd_2.toDebugString())



before1= datetime.now() 
print (rdd_2.take(10)) 
after1 = datetime.now() 

timetaken1 = after1 - before1 

print("time take =", timetaken1 ) 


rdd_nopart2 = rdd_nopart.map(lambda x : x+30) 


before1= datetime.now() 
print (rdd_nopart2.take(10)) 
after1 = datetime.now() 

timetaken1 = after1 - before1 

print("time take without  =", timetaken1 ) 










