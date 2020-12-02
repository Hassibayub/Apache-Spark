from pyspark import SparkConf, SparkContext 

conf = SparkConf().setMaster("local[*]").setAppName("most popular superheo")
sc = SparkContext(conf= conf)

def parseNames(val):
    data = val.split(" ", 1)
    id = int(data[0])
    name = data[1]
    return (id,name)

def calculateCoOccurance(val):
    data = val.split()
    heroid = int(data[0])
    otherOccurrence = int(len(data)) - 1
    return (heroid, otherOccurrence)

heroNamesRaw = sc.textFile("./Marvel Names.txt")
heroNames = heroNamesRaw.map(parseNames)
# output: (1, 'superman')

popularHeroRaw = sc.textFile('./Marvel Graph.txt')
popularHero = popularHeroRaw.map(calculateCoOccurance) 
# output: (id, occur)

sumPoplarHero = popularHero.reduceByKey(lambda x,y: x+y)
flipped = sumPoplarHero.map(lambda x: (x[1],x[0]))
top1 = flipped.max()
# print(top1)

top1Name = heroNames.lookup(top1[1])[0]
print(top1Name)
print("{} is most popular superhero, appeared {} times.".format(top1Name,top1[0]))


