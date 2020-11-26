from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FakeFriends")
sc = SparkContext(conf= conf)

def parseData(val):
    data = val.split()
    age = int(data[2])
    friends = int(data[3])
    return (age,friends)


raw = sc.textFile(r"G:\Shared drives\Unlimited\Python Scripts\Apache Spark\fakefriends.csv")

datapair = raw.map(lambda x: ( int(x.split(",")[2]), int(x.split(",")[3]) ))
# datapaint (age, friends)

aggData  = datapair.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y : (x[0] + y[0], x[1]+y[1] ))
# # aggData (eachAge , (totalfriends, counter))

avgData = aggData.mapValues(lambda x: x[0]/x[1])
# # # avgData (age, avgFriends)

dataCollected = avgData.collect()

for age, avgFrinds in dataCollected:
    print("At Age {}, Avg Friends {}".format(age, int(avgFrinds)))