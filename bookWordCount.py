from pyspark import SparkConf, SparkContext 
import collections
import re 

conf = SparkConf().setMaster("local").setAppName("bookWordCount")
sc = SparkContext(conf= conf)

raw = sc.textFile('./Book.txt')
words = raw.flatMap(lambda x: re.compile(r'\W+', re.UNICODE).split(x.lower()))
counter = words.countByValue()
counter = dict(sorted(counter.items()))

# print(counter)
for word, count in counter.items():
    print("{}, {} times.".format(word, count))