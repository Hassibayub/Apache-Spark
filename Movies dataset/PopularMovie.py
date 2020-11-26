from typing import Counter
from pyspark import SparkConf, SparkContext 
import os 
conf = SparkConf().setMaster("local").setAppName("Most Popular Movie")
sc = SparkContext(conf= conf)

# objective: what movie watched most often?
raw = sc.textFile("./Movies dataset/ml-100k/u.data")
movies = raw.map(lambda x: x.split()[1])
counter = movies.countByValue()
counter = dict(counter)

counter = sorted(counter.items(), key=lambda items: items[1])
# print(counter)

for movie, counter in counter.items():
    print("{}, been watched for '{}' times".format(movie, counter))