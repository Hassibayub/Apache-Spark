from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/Users/Hassi/AppData/Local/Temp").appName("MoviesRatings").getOrCreate()

def loadMovieNames():
    with open("./Movies dataset/ml-100k/u.item") as file:
        movies = {}
        for line in file:
            data = line.split("|")
            id = int(data[0])
            name = data[1]
            movies[id] = name
        
        return movies
            

movieDict = loadMovieNames()

raw  = spark.sparkContext.textFile("./Movies dataset/ml-100k/u.data")
ratings = raw.map(lambda x: Row(id = int(x.split()[1])) )

dataframe = spark.createDataFrame(ratings).cache()

topMoviesID = dataframe.groupBy("id").count().orderBy("count", ascending=False).cache()

top20 = topMoviesID.take(20)

for result in top20:
    print("{}: {}".format( movieDict[result[0]], result[1] ))


spark.stop()