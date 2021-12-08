from pyspark import SparkConf, SparkContext



def main():
    '''Program entry point'''

    # initialize spark context
    conf = SparkConf()
    conf.setAppName("PySpark WordCount")
    conf.setMaster("local")   # setMaster(spark://head_node:56887') 
    sc = SparkContext(conf = conf)
    
    # only output when error 
    sc.setLogLevel("ERROR")
    
    # TODO : read conf from file 


    # read from input directory 
    # filesRDD = sc.wholeTextFiles("inputDir")

    # read single file into RDD and create only one partition 
    logFileRDD =sc.textFile("inputDir/input_1.txt", minPartitions=None) 
 
    # check 
    print ( type ( logFileRDD )) 
    print ( logFileRDD.toDebugString())
    print ( logFileRDD.getNumPartitions()) 
 
    print ( "Count of lines -> %i " % logFileRDD.count()) 

    # Filter out non-empty lines from the loaded file 
   logFileRDD = logFileRDD.filter(lambda x: len(x) > 0)

    print ( "Count of non-empty lines -> %i " % logFileRDD.count()) 


    # Only consider lines that have special word 
    # A new RDD is returned containing the elements, which satisfies the function inside the filter.
    logFileRDD = logFileRDD.filter(lambda x : "__KEYWORD__" in x ) 

    # Number of elements in the RDD is returned.
    print ( "Number of lines with keyword -> %i" % (logFileRDD.count()) ) 

    print ( logFileRDD.collect()) 

    # Split the logFileRDD on space
    words = logFileRDD.flatMap(lambda x: x.split(' '))




    # sort by some value 
    # better way to sort them by value but print by key 
    wordCounts = words.map(lambda x:(x, 1))\
             .reduceByKey(lambda x,y: x+y)\
             .sortBy(lambda x: x[1], ascending=False)

    # instead of sortBy we can also use 
    #             .map(lambda x: (x[1], x[0]))\
    #            .sortByKey(False)               

    print (wordCounts.collect() ) 


# Count the occurance of each words
# to swap key and value 
# to sort by value ( which has now become key ) 
#'''

# output to directory 

    for word,count  in  wordCounts.collect() :
        print ("%s -> %i" % ( word , count ) ) 
    



# View the file after filter

    
    # save the counts to output
    wordCounts.saveAsTextFile("outputDir") 
    


#### avoid getting everything to one 

    # Stopping Spark-Session and Spark context
    sc.stop()
    #spark.stop()




if __name__ == "__main__":
    main()



