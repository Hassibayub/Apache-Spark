from pyspark import SparkConf, SparkContext 
import collections
import time

start = time.perf_counter()

conf = SparkConf().setMaster("local").setAppName("RatingCounter")
sc = SparkContext(conf = conf)

try:
    lines = sc.textFile("./ml-100k/u.data")
    rating = lines.map(lambda x: x.split()[2])
    result = rating.countByValue()
except:
    lines = sc.textFile("./Movies dataset/ml-100k/u.data")
    rating = lines.map(lambda x: x.split()[2])
    result = rating.countByValue()
    

sortedResults = collections.OrderedDict(sorted( result.items() )) 

for key, value in sortedResults.items():
    print("{}, {}".format(key, value))
    
print("Total time took: ", time.perf_counter() - start)