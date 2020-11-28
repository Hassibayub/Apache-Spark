from pickle import load
from typing import Counter
from pyspark import SparkConf, SparkContext 

def loadMoviesName():
    movieName = {}
    with open("./Movies dataset/ml-100k/u.item") as file:
        for row in file:
            data = row.split("|")
            id = int(data[0])
            name = data[1]
            movieName[id] = name
    return movieName

conf = SparkConf().setMaster("local").setAppName("Most Popular Movie")
sc = SparkContext(conf= conf)

nameDict = sc.broadcast(loadMoviesName())

raw = sc.textFile("./Movies dataset/ml-100k/u.data")
movies = raw.map(lambda x: (int(x.split()[1]),1) )
movieCounts = movies.reduceByKey(lambda x,y:x+y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda x : (nameDict.value[ x[1]], x[0] ) )

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
