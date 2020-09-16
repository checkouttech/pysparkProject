from pyspark import SparkContext
from pyspark import SparkFiles


#logFile = "file:/opt/spark/README.md"

#logData = sc.textFile(logFile).cache()



finddistance = "/opt/spark/examples/src/main/python/kmeans.py"
finddistancename = "kmeans.py"
sc = SparkContext("local", "SparkFile App")
sc.addFile(finddistance)
print "Absolute Path -> %s" % SparkFiles.get(finddistancename)
print "Absolute Path -> %s" % SparkFiles.get(finddistance)





