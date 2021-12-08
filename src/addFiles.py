

from pyspark import SparkContext
from pyspark import SparkFiles
finddistance = "finddistance.R"
finddistancename = "finddistance.R"
sc = SparkContext("local", "SparkFile App")
sc.addFile(finddistance)
print ("Absolute Path -> %s" % SparkFiles.get(finddistancename)) 




