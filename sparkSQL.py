from itertools import tee
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/Users/Hassi/AppData/Local/Temp").appName("SparkSQL").getOrCreate()

def rawParser(val):
    data = val.split(',')
    # print("||||||||||||||||| ", data, " |||||||||||||||||")
    id = int(data[0])
    name = data[1]
    age = int(data[2])
    friends = int(data[3])
    return Row( id=id, name=name, age=age, friends=friends )


raw = spark.sparkContext.textFile("fakefriends.csv")
people = raw.map(rawParser)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

teenagers = spark.sql('''
                      SELECT * 
                      FROM people
                      WHERE age >= 13 AND age <= 19;
                      ''')

# for teenager in teenagers.collect():
#     print(teenager)


schemaPeople.groupBy("name").count().orderBy("count").show()