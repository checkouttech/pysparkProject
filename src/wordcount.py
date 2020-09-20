
import sys
 
from pyspark import SparkContext, SparkConf
 
if __name__ == "__main__":
    
    # create Spark context with necessary configuration
    #  sc = SparkContext.getOrCreate();
    sc = SparkContext("local","PySpark Word Count Exmaple")
    
    # to reduce verbosity 
    sc.setLogLevel("WARN")

    # input filename 
    input_filename = "/home/vagrant/foo.txt"

    # read data from text file and create lines RDD  
    lines =  sc.textFile(input_filename) 
    
    print ( lines.collect()) 

    # split each line and create words RDD
    words = lines.flatMap(lambda line: line.split(" "))

    print ( words.collect()) 

    # filter out spaces 
    words  = words.filter(lambda word: word != '') 


    # create a new RDD with numeric 1 for each word generate "word , 1"  
    wordMap   =  words.map(lambda word: (word, 1)) 
    
    print ( wordMap.collect()) 


    # count the occurrence of each word
    wordCounts = wordMap.reduceByKey(lambda a, b: a + b) 
    
    print ( wordCounts.collect()) 

    
    # filter by value 


    # create a new RDD with decreasing order 
    # sortBy(lambda (x, y): y, x) is a function call with 2 arguments in Python.

    #wordSortedByCount = wordCounts.sortBy(lambda (word,count) : (count, word) , ascending=False)  
    wordSortedByCount = wordCounts.sortBy(lambda (word,count) : count , ascending=False)


    # pick top 10 
    top10_wordSortedByCount = wordSortedByCount.take(10) 

    print ( "top10_wordSortedByCount ---" ) 
    print ( top10_wordSortedByCount ) 

     
    # if we want to put number before word 
    #wordSortedByCount = wordCounts.map( lambda x: ( x[1], x[0])).sortByKey(False) 

    print ( "wordSortedByCount -- ") 
    print ( wordSortedByCount.collect()) 



    # save the counts to output
    #wordCounts.saveAsTextFile("/tmp/wc.out")
    wordSortedByCount.saveAsTextFile("/tmp/wc.out")


    # stop context 
    sc.stop()





# input_file = sc.textFile("/path/to/text/file")
# map = input_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
# counts = map.reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("/path/to/output/")











