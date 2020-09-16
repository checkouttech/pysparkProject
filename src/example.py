




from pyspark import SparkContext

# sc = SparkContext("local", "First App")
sc = SparkContext.getOrCreate()


logFile = "file:/opt/spark/README.md"

logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()

numBs = logData.filter(lambda s: 'b' in s).count()


print "Lines with a: %i, lines with b: %i" % (numAs, numBs)



