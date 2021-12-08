from datetime import datetime
from pyspark import SparkContext

from random import randint


sc = SparkContext("local", "spark operation") 
sc.setLogLevel("ERROR")



# create a list of random numbers between 10 to 1000
#my_large_list = [randint(10,1000) for x in range(0,20000000)]
my_large_list = [i for i in range(0,20000000)]

# # create two list of different partitions 
# 1 partition 
my_large_list_one_partition = sc.parallelize(my_large_list,numSlices=1)

# 10 partition 
my_large_list_ten_partition = sc.parallelize(my_large_list,numSlices=10)



# check number of partitions
print(my_large_list_one_partition.getNumPartitions())
print(my_large_list_ten_partition.getNumPartitions())

# >> 1

# filter numbers greater than equal to 200
my_large_list_one_partition = my_large_list_one_partition.filter(lambda x : int(x) >= 200)
my_large_list_ten_partition = my_large_list_ten_partition.filter(lambda x : int(x) >= 200)


# count the number of elements in filtered list




before1= datetime.now() 
print(my_large_list_one_partition.first())
after1 = datetime.now() 
# >> 16162207
timetaken1 = after1 - before1 
print("time take =", timetaken1 ) 




before1= datetime.now() 
print(my_large_list_ten_partition.first())
after1 = datetime.now() 
# >> 16162207
timetaken1 = after1 - before1 
print("time take =", timetaken1 ) 




# code was run in a jupyter notebook
# to calculate the time taken to execute the following command
#%%time

# count the number of elements in filtered list

#before1= datetime.now() 
#print(my_large_list_one_partition.count())
#after1 = datetime.now() 
# >> 16162207



#print('output =',rdd1.reduce(lambda x,y:x+y))





