import time 
from  timeit import timeit 
from datetime import datetime


from pyspark import SparkContext

sc = SparkContext("local", "spark operation") 
sc.setLogLevel("ERROR")


numbers_rdd = sc.parallelize ( [4, 8, 2, 2, 4, 7, 0, 3, 3, 9, 2, 6, 0, 0, 1, 7, 5, 1, 9, 7] ) 


# sum ofall numbers 
print ("sum of rdd -> %i " % ( numbers_rdd.sum()))

# sum using reduce 
sum = numbers_rdd.reduce( lambda x,y : x + y ) 
print ("sum of rdd -> %i " % ( sum )) 



# collect all 
print ("collect all elements of rdd -> %s"  % ( numbers_rdd.collect()))

# first element 
print ("first elements of rdd -> %s"  % ( numbers_rdd.first()))

# take first 3 and create a new list ( not an rdd ) 
new_rdd = numbers_rdd.take(3) 
print ("first three elements of rdd -> %s"  % (new_rdd))
print ( type(numbers_rdd ))
print ( type(new_rdd))

# Finding maximum element by reducing
highest_number =  numbers_rdd.reduce( lambda x,y : x if x > y else y )
print ("biggest number in rdd -> %i " % ( highest_number) )


# Finding the longest word in a blob of text
words = 'These are some of the best Macintosh computers ever'.split(' ')
wordRDD = sc.parallelize( words) 

longest_word = wordRDD.reduce ( lambda x,y : x if len(x) > len(y) else y ) 
print ("longest word in a rdd -> %s " % ( longest_word )) 


# Use the filter for logic-based filtering
# Return RDD with elements (greater than zero) divisible by 3
div_by_3_rdd = numbers_rdd.filter(lambda x : x % 3 ==0 and x != 0 )
print ( div_by_3_rdd.collect() ) 


#Write regular Python functions to use with reduce()
def largerThan(x,y): 

    if len(x) > len(y) :
        return x
    elif len(y ) > len(x) :
        return y 
    return x

longest_word_using_function = wordRDD.reduce(largerThan) 

print ("longest word in a an rdd -> %s " % ( longest_word )) 


# square of number using map 

squared_rdd = numbers_rdd.map(lambda x : x*x ) 

print ("squred rdd is -> %s " , ( squared_rdd.collect()))


# groupby returns an RDD of grouped elements (iterable) as per a given group operation


# groupby 
grouped_by_numbers = numbers_rdd.groupBy(lambda x : x % 2 == 0 ) 

print ("grouped by rdd is -> %s " , ( grouped_by_numbers.collect()))  

print ( sorted ( [(x, sorted(y)) for (x, y) in  grouped_by_numbers.collect() ])) 
print ( sorted ( grouped_by_numbers.collect() )) 



# histogram 
print ("histogram ") 
print ( ([x for x in range(0,100,10)]) ) 
B = squared_rdd.collect()
print (B ) 
print ( numbers_rdd.histogram([x for x in range(0,100,10)]) ) 
#print ( B.histogram([x for x in range(0,100,10)]) ) 





# cache 
# Make a RDD with 1 million elements
before1= datetime.now() 
rdd1 =  sc.parallelize(range(1000000))  
after1 = datetime.now() 

timetaken1 = after1 - before1 

print("time take =", timetaken1 ) 


before1= datetime.now() 
print('output =',rdd1.reduce(lambda x,y:x+y).cache() )
after1 = datetime.now() 

timetaken1 = after1 - before1 

print("time take =", timetaken1 ) 


before1= datetime.now() 
print('output =',rdd1.reduce(lambda x,y:x+y))
after1 = datetime.now() 

timetaken1 = after1 - before1 

print("time take =", timetaken1 ) 


