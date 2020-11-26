from pyspark import SparkContext, SparkConf 

conf = SparkConf().setMaster("local").setAppName("1800MinWeatherConter")
sc = SparkContext(conf=conf)

def parse_data(row):
    data = row.split(",")
    id = data[0]
    status = data[2]
    temp = int(data[3]) * 0.1
    return (id, status, temp) 

raw = sc.textFile("./1800.csv") # file loaded
data = raw.map(parse_data) # parse data
minTemp = data.filter(lambda x: "TMIN" in x[1]) # collect `TMIN` data only
tempdata = minTemp.map(lambda x: (x[0], x[2]) ) # remoing `TMIN` string
counted = tempdata.reduceByKey(lambda x,y: min(x,y))
print(counted.collect())